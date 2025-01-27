from dataclasses import dataclass
from typing import Dict, Optional
from rich.console import Console
from dotenv import load_dotenv
import urllib.parse
import aiofiles
import argparse
import asyncio
import aiohttp
import backoff
import shutil
import httpx
import json
import os
import re

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
)

load_dotenv()

Parser = argparse.ArgumentParser(
    description="Fetch and download content from various platforms",
    prog="NeoFetch",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)

Parser.add_argument(
    "--dry-run",
    help="Run without downloading any files",
    default=False,
    type=bool,
    nargs='?',
)

LOG_LEVEL = 0  # 0: Debug, 1: Info, 2: Warning, 3: Error, 4: Critical

class RichLogger:
    def __init__(self, Name=__name__):
        self.Console = Console(
            markup=True,
            log_time=False,
            force_terminal=True,
            width=140,
            log_path=False
        )
    
    LogLevels = {
        'DEBUG': 1,
        'INFO': 2,
        'WARNING': 3,
        'ERROR': 4,
        'CRITICAL': 5
    }

    def Debug(self, Message):
        if LOG_LEVEL <= self.LogLevels['DEBUG']:
            self.Console.log(f"[bold blue]DEBUG:   [/bold blue] {Message}")

    def Info(self, Message):
        if LOG_LEVEL <= self.LogLevels['INFO']:
            self.Console.log(f"[bold green]INFO:    [/bold green] {Message}")

    def Warning(self, Message):
        if LOG_LEVEL <= self.LogLevels['WARNING']:
            self.Console.log(f"[bold yellow]WARNING: [/bold yellow] {Message}")

    def Error(self, Message):
        if LOG_LEVEL <= self.LogLevels['ERROR']:
            self.Console.log(f"[bold red]ERROR:   [/bold red] {Message}")

    def Critical(self, Message):
        if LOG_LEVEL <= self.LogLevels['CRITICAL']:
            self.Console.log(f"[bold magenta]CRITICAL:[/bold magenta] {Message}")

Logger = RichLogger(__name__)

async def ReadConfig():
    async with aiofiles.open('config.json', 'r') as f:
        return json.loads(await f.read())

Config = asyncio.run(ReadConfig())

class FavoriteFetcher:
    def __init__(self, Platform):
        self.Platform = Platform
        self.Url = f'https://{Platform}.su/api/v1/account/favorites?type=artist'

    @classmethod
    async def Create(cls, Platform):
        self = cls(Platform)
        await self.Initialize()
        return self

    async def Initialize(self):
        async with httpx.AsyncClient() as Client:
            Client.headers.update({
                'accept': 'application/json',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'Mozilla/5.0 (SMART-TV; Linux; Tizen 5.0) AppleWebKit/537.36'
            })

            Client.cookies.set(
                'session',
                os.getenv('COOMER_SESS') if self.Platform == 'coomer' else os.getenv('KEMONO_SESS'),
                domain=f'{self.Platform}.su',
                path='/'
            )

            Response = await Client.get(self.Url)
            
            if Response.status_code == 200:
                Data = Response.json()

                for Service in ['onlyfans', 'fansly', 'patreon', 'subscribestar', 'fanbox', 'gumroad']:
                    if 'ids' not in Config[Service]:
                        Config[Service]['ids'] = []
                    if 'names' not in Config[Service]:
                        Config[Service]['names'] = []

                for Item in Data:
                    Service = Item.get('service')
                    CreatorId = Item.get('id')
                    CreatorName = Item.get('name')
                    Config[Service]['ids'].append(CreatorId)
                    Config[Service]['names'].append(CreatorName)

class InsufficientDiskSpaceError(Exception):
    """Exception raised when there is not enough disk space."""
    pass

async def CheckDiskSpace(RequiredBytes: int = 5e+9) -> bool:
    """Check if sufficient disk space is available"""
    if shutil.disk_usage('/').free < RequiredBytes:
        Logger.Critical(f"Insufficient disk space! Only {AsyncDownloadManager.HumanizeBytes(shutil.disk_usage('/').free)} remaining")
        raise InsufficientDiskSpaceError("Not enough disk space to continue.")
    return True

@dataclass
class DownloadItem:
    """Represents a single file to be downloaded"""
    FileHash: str
    FileUrl: str 
    SavePath: str
    Platform: str
    Creator: str
    FileSize: Optional[int] = None
    RetryCount: int = 0

class HashManager:
    """Handles loading and saving cached hashes."""
    def __init__(self, cache_file: str = 'cached_hashes.json'):
        self.cache_file = cache_file
        self.cached_hashes = {}
        self.LoadCache()

    def LoadCache(self):
        """Load cached hashes from the cache file."""
        try:
            with open(self.cache_file, 'r') as f:
                self.cached_hashes = json.load(f)
                TotalHashes = sum(len(hashes) for platform in self.cached_hashes.values() 
                                  for hashes in platform.values())
                Logger.Debug(f"∙ Loaded {TotalHashes} cached hashes")
        except (FileNotFoundError, json.JSONDecodeError):
            self.cached_hashes = {}
            Logger.Debug("∙ No existing cache found, starting fresh")

    def SaveHashes(self, new_hashes: Dict[str, Dict[str, list[str]]]):
        """Save new hashes to the cache file."""
        try:
            for platform, creators in new_hashes.items():
                if platform not in self.cached_hashes:
                    self.cached_hashes[platform] = {}
                for creator, hashes in creators.items():
                    if creator not in self.cached_hashes[platform]:
                        self.cached_hashes[platform][creator] = []
                    self.cached_hashes[platform][creator].extend(
                        [h for h in hashes if h not in self.cached_hashes[platform][creator]]
                    )
            with open(self.cache_file, 'w') as f:
                json.dump(self.cached_hashes, f, indent=4)
            Logger.Debug("∙ Saved new hashes to cache")
        except Exception as e:
            Logger.Error(f"Failed to save cache: {e}")

    def HasHash(self, platform: str, creator: str, file_hash: str) -> bool:
        """Check if a hash exists in the cache."""
        return (
            platform in self.cached_hashes and
            creator in self.cached_hashes[platform] and
            file_hash in self.cached_hashes[platform][creator]
        )

class AsyncDownloadManager:
    """Handles async downloads with retry logic and progress tracking"""
    def __init__(self, FileList: list, MaxConcurrent: int = 32):
        self.Items = [DownloadItem(FileHash=f[0][0], FileUrl=f[0][1], SavePath=f[0][2], 
                                 Platform=f[1], Creator=f[2]) for f in FileList]
        self.MaxConcurrent = MaxConcurrent
        self.ProcessedHashes = set()
        self.DryRun = Config.get('dry_run', False)
        self.TotalFiles = len(FileList)
        self.CompletedFiles = 0
        self.FailedFiles = 0
        self.TotalBytes = 0
        self.NewHashes = {}
        self.Lock = asyncio.Lock()
        self.HashManager = HashManager()  # Initialize HashManager

    async def Start(self) -> bool:
        try:
            self.Semaphore = asyncio.Semaphore(self.MaxConcurrent)
            async with aiohttp.ClientSession() as Session:
                self.Session = Session
                
                Logger.Info(f"∙ Downloading {self.TotalFiles} files...")

                def GetPlatformColor(Platform):
                    PlatformText = Config['platform_names'].get(Platform, '')
                    ColorMatch = re.search(r'\[([a-z0-9_]+)\]', PlatformText.lower())
                    return ColorMatch.group(1) if ColorMatch else 'white'

                ProgressColumns = [
                    TextColumn("{task.fields[creator]}", style=f"bold {GetPlatformColor(self.Items[0].Platform)}"),
                    BarColumn(bar_width=None),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TextColumn("•"),
                    TextColumn("[blue]{task.fields[file]}"),
                    TextColumn("•"), 
                    TextColumn("{task.fields[size]}"),
                    TextColumn("•"),
                    MofNCompleteColumn(),
                    TimeRemainingColumn(),
                ]

                Progress_Bar = Progress(*ProgressColumns, auto_refresh=False, console=RichLogger().Console, expand=True)
                
                with Progress_Bar as progress:
                    task_id = progress.add_task(
                        "",
                        total=self.TotalFiles,
                        file="Starting...",
                        size="0 B",
                        platform=self.Items[0].Platform if self.Items else "",
                        creator=self.Items[0].Creator if self.Items else ""
                    )

                    # Create download tasks for all files
                    tasks = []
                    for item in self.Items:
                        if not self.HashManager.HasHash(item.Platform, item.Creator, item.FileHash):
                            tasks.append(self.DownloadFile(item, progress, task_id))
                        else:
                            Logger.Debug(f"∙ Skipping {item.FileHash} as it is already cached")
                    
                    if tasks:
                        await asyncio.gather(*tasks)

            self.HashManager.SaveHashes(self.NewHashes)
            Logger.Debug(f"∙ Saved {sum(len(hashes) for platform in self.NewHashes.values() for hashes in platform.values())} new hashes to cache")
            return True
            
        except InsufficientDiskSpaceError:
            Logger.Error("Insufficient disk space encountered.")
            self.HashManager.SaveHashes(self.NewHashes)
            total_saved = sum(len(hashes) for platform in self.NewHashes.values() for hashes in platform.values())
            Logger.Debug(f"∙ Saved {total_saved} hashes before exiting.")
            return False
        except Exception as Error:
            Logger.Error(f"Download manager encountered an error: {str(Error)}")
            self.HashManager.SaveHashes(self.NewHashes)
            total_saved = sum(len(hashes) for platform in self.NewHashes.values() for hashes in platform.values())
            Logger.Debug(f"∙ Saved {total_saved} hashes before exiting.")
            return False

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    async def DownloadFile(self, Item: DownloadItem, progress, TaskId) -> bool:
        try:
            async with self.Semaphore:
                await CheckDiskSpace()  # This may raise InsufficientDiskSpaceError

                try:
                    # Get file size first
                    async with self.Session.head(Item.FileUrl, allow_redirects=True) as Response:
                        if Response.status != 200:
                            #Logger.Warning(f"Failed to get size for {Item.FileHash}: {Response.status}")
                            return False
                        FileSize = int(Response.headers.get('content-length', 0))
                        Item.FileSize = FileSize

                    if self.DryRun:
                        await asyncio.sleep(0.1)
                        async with self.Lock:
                            self.CompletedFiles += 1
                            self.ProcessedHashes.add(Item.FileHash)
                            progress.update(TaskId, completed=self.CompletedFiles, 
                                            file=f"{Item.FileHash[:20]}...", 
                                            size=self.HumanizeBytes(FileSize))
                            progress.refresh()  # Add refresh here for dry run
                        return True

                    # Real download
                    TempPath = f"{Item.SavePath}.downloading"
                    async with self.Session.get(Item.FileUrl) as Response:
                        if Response.status == 200:
                            os.makedirs(os.path.dirname(Item.SavePath), exist_ok=True)
                            
                            async with aiofiles.open(TempPath, 'wb') as f:
                                Downloaded = 0
                                async for Chunk in Response.content.iter_chunked(1024*1024):
                                    if Chunk:
                                        await f.write(Chunk)
                                        Downloaded += len(Chunk)
                                        async with self.Lock:
                                            progress.update(TaskId, 
                                                            file=f"{Item.FileHash[:20]}...",
                                                            size=self.HumanizeBytes(Downloaded))
                                progress.refresh()  # Add refresh here for chunk updates

                            # Rename from .downloading to final name
                            os.rename(TempPath, Item.SavePath)
                            
                            # Track hash for caching
                            async with self.Lock:
                                if Item.Platform not in self.NewHashes:
                                    self.NewHashes[Item.Platform] = {}
                                if Item.Creator not in self.NewHashes[Item.Platform]:
                                    self.NewHashes[Item.Platform][Item.Creator] = []
                                self.NewHashes[Item.Platform][Item.Creator].append(Item.FileHash)
                                
                                self.CompletedFiles += 1
                                self.ProcessedHashes.add(Item.FileHash)
                                self.TotalBytes += Downloaded
                                progress.update(TaskId, completed=self.CompletedFiles)

                            return True

                    return False

                except SystemExit as Error:
                    raise Error  # Re-raise disk space error
                except Exception: # as Error:
                    #Logger.Warning(f"Download failed for {Item.FileHash}: {str(Error)}")
                    return False

        except InsufficientDiskSpaceError:
            raise  # Propagate the exception to be handled in Start()
        except Exception as Error:
            self.FailedFiles += 1
            if os.path.exists(f"{Item.SavePath}.downloading"):
                os.remove(f"{Item.SavePath}.downloading")
            raise Error

    @staticmethod
    def HumanizeBytes(Bytes: int) -> str:
        for Unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if Bytes < 1024:
                return f"{Bytes:.2f} {Unit}"
            Bytes /= 1024

class Fetcher:
    def __init__(self, Platform, Id, Name, DirectoryName, CachedHashes, CreatorLimit, GlobalLimit):
        self.Page = 0
        self.Client = httpx.AsyncClient()
        
        # Rest of init remains same
        self.Platform = Platform
        self.Id = Id
        self.Name = Name
        self.DirectoryName = DirectoryName
        self.CachedHashes = CachedHashes
        self.CreatorLimit = CreatorLimit
        self.GlobalLimit = GlobalLimit
        self.Result = {self.Platform: {self.Id: []}}
        self.FilesDownloaded = 0

        self.CachedHashes = CachedHashes

        #self.FetcherInstance = FavoriteFetcher('coomer')  # Use shared instance for hash checking

        self.Params = None

        if self.Platform == 'rule34':
            self.Params = {
                'page': 'dapi',
                's': 'post',
                'q': 'index',
                'json': '1',
                'tags': self.Id,
                'pid': self.Page,
                'limit': 1000  # Rule34 max is 1000 per page
            }
        elif self.Platform == 'e621':
            self.Params = {
                'limit': 320,  # e621 max is 320 per page
                'tags': self.Id,
                'page': self.Page,
                'login': os.getenv('E621_LOGIN'),
                'api_key': os.getenv('E621_API_KEY')
            }

        self.ParamsLimit = self.Params['limit'] if self.Params else 0
        self.Params = urllib.parse.urlencode(self.Params, safe='+') if self.Platform in ['rule34', 'e621'] else self.Params  # Avoid '+' encoding in params
        self.DryRun = Config.get('dry_run', False)
        self.CacheManager = HashManager()  # Initialize HashManager

    def ExtractHash(self, Url):  # Extract hash from URL / <hash> .png
        if not Url:
            return None
        # Extract the filename from the URL and split on last dot
        Filename = Url.split('/')[-1]
        return Filename.rsplit('.', 1)[0]

    async def FetchUrl(self, Url: str, Params: Dict = None) -> Dict:
        try:
            Response = await self.Client.get(Url, params=Params)
            if Response.status_code == 200:
                return Response.json(), Response.status_code
            return None, Response.status_code
        except Exception:
            return None, None

    async def Scrape(self):
        if self.Platform == 'rule34':
            BaseParams = dict(urllib.parse.parse_qsl(self.Params))
            while self.GlobalLimit > 0 and self.CreatorLimit > 0:
                BaseParams["pid"] = self.Page
                ReEncodedParams = urllib.parse.urlencode(BaseParams, safe="+")
                Response, StatusCode = await self.FetchUrl('https://api.rule34.xxx/index.php', ReEncodedParams)
                
                try:
                    _ = 0
                    if Response:
                        Data = Response
                        if not Data or (isinstance(Data, list) and len(Data) == 0):
                            break
                        
                        for Post in Data:
                            if self.GlobalLimit > 0 and self.CreatorLimit > 0:
                                FileUrl = Post.get('file_url')
                                FileHash = self.ExtractHash(FileUrl)

                                if FileHash and self.CacheManager.HasHash(self.Platform, self.Id, FileHash):
                                    #Logger.Debug(f"∙ Skipping {FileHash} as it is already cached")
                                    continue

                                if FileHash:
                                    #Logger.Debug(f'∙ Found New File {FileHash[:40]}⋯ At Page {self.Page+1}')
                                    FileData = [FileHash, FileUrl, f'{self.DirectoryName}/{FileHash}{os.path.splitext(FileUrl)[1]}']
                                    self.Result[self.Platform][self.Id].append(FileData)
                                    self.GlobalLimit -= 1
                                    self.CreatorLimit -= 1
                                    self.FilesDownloaded += 1
                                    _ += 1
                        
                        if _ < self.ParamsLimit:
                            #Logger.Info(f'Page {self.Page+1} → {_} files')
                            break
                        #Logger.Info(f'Page {self.Page+1} → {self.FilesDownloaded} files')
                    else:
                        if StatusCode != 200:
                            #Logger.Error(f'No response or bad status ({StatusCode}) at page {self.Page+1}')
                            #Logger.Error(f'Params: {ReEncodedParams}')
                            #Logger.Error(f'URL: {Response.url}')
                            break
                        else:
                            #Logger.Error(f'No data at page {self.Page+1}')
                            break
                except Exception:
                    #Logger.Error(f'Error processing page {self.Page+1}: {e}')
                    break

                self.Page += 1

        ############################################################
        #                                                          #
        #                           e621                           #
        #                                                          #
        ############################################################

        elif self.Platform == 'e621':
            BaseParams = dict(urllib.parse.parse_qsl(self.Params))
            while self.GlobalLimit > 0 and self.CreatorLimit > 0:
                BaseParams["page"] = self.Page + 1
                ReEncodedParams = urllib.parse.urlencode(BaseParams, safe="+")
                Response, StatusCode = await self.FetchUrl("https://e621.net/posts.json", ReEncodedParams)
                
                try:
                    if Response and 'posts' in Response:
                        Posts = Response['posts']
                        if not Posts or len(Posts) == 0:
                            break

                        _ = 0
                        for Post in Posts:
                            if self.GlobalLimit > 0 and self.CreatorLimit > 0:
                                FileUrl = Post.get('file', {}).get('url')  # Nested file URL
                                if not FileUrl:
                                    continue
                                    
                                FileHash = self.ExtractHash(FileUrl)
                                if FileHash and self.CacheManager.HasHash(self.Platform, self.Id, FileHash):
                                    #Logger.Debug(f"∙ Skipping {FileHash} as it is already cached")
                                    continue

                                if FileHash:
                                    #Logger.Debug(f'∙ Found New File {FileHash[:40]}⋯ At Page {self.Page+1}')
                                    FileData = [FileHash, FileUrl, f'{self.DirectoryName}/{FileHash}{os.path.splitext(FileUrl)[1]}']
                                    self.Result[self.Platform][self.Id].append(FileData)
                                    self.GlobalLimit -= 1
                                    self.CreatorLimit -= 1
                                    self.FilesDownloaded += 1
                                    _ += 1

                        if _ < self.ParamsLimit:
                            #Logger.Info(f'Page {self.Page+1} → {_} files')
                            break

                        #Logger.Info(f'Page {self.Page+1} → {self.FilesDownloaded} files')
                    else:
                        #Logger.Error(f'No response or bad status ({StatusCode}) at page {self.Page+1}')
                        break
                        
                except Exception:
                    #Logger.Error(f'Error processing page {self.Page+1}: {e}')
                    break

                self.Page += 1

        ############################################################
        #                                                          #
        #                Onlyfans, Fansly, Patreon                 #
        #              SubscribeStar, Gumroad, Fanbox              #
        #                                                          #
        ############################################################

        else:
            Hoster = 'coomer' if self.Platform in ['onlyfans', 'fansly'] else 'kemono'
            Response, StatusCode = await self.FetchUrl(f'https://{Hoster}.su/api/v1/{self.Platform}/user/{self.Id}')
            
            try:
                if Response and isinstance(Response, list):  # Change here - response is a list     
                    _ = 0
                    for Post in Response:
                        if self.GlobalLimit > 0 and self.CreatorLimit > 0:
                            # Handle attachments
                            for Attachment in Post.get('attachments', []):
                                if self.GlobalLimit <= 0 or self.CreatorLimit <= 0:
                                    break
                                    
                                FileUrl = f'https://{Hoster}.su{Attachment.get("path")}'
                                FileHash = self.ExtractHash(FileUrl)
                                
                                if FileHash and self.CacheManager.HasHash(self.Platform, self.Id, FileHash):
                                    #Logger.Debug(f"∙ Skipping {FileHash} as it is already cached")
                                    continue

                                if FileHash:
                                    #Logger.Debug(f'∙ Found New File {FileHash[:40]}⋯')
                                    FileData = [FileHash, FileUrl, f'{self.DirectoryName}/{FileHash}{os.path.splitext(FileUrl)[1]}']
                                    self.Result[self.Platform][self.Id].append(FileData)
                                    self.GlobalLimit -= 1
                                    self.CreatorLimit -= 1
                                    self.FilesDownloaded += 1
                                    _ += 1

                            # Handle main file
                            File = Post.get('file', {})
                            if File:
                                FileUrl = f'https://{Hoster}.su{File.get("path")}'
                                FileHash = self.ExtractHash(FileUrl)
                                
                                if FileHash and self.CacheManager.HasHash(self.Platform, self.Id, FileHash):
                                    #Logger.Debug(f"∙ Skipping {FileHash} as it is already cached")
                                    continue

                                if FileHash:
                                    #Logger.Debug(f'∙ Found New File {FileHash[:40]}⋯')
                                    FileData = [FileHash, FileUrl, f'{self.DirectoryName}/{FileHash}{os.path.splitext(FileUrl)[1]}']
                                    self.Result[self.Platform][self.Id].append(FileData)
                                    self.GlobalLimit -= 1
                                    self.CreatorLimit -= 1 
                                    self.FilesDownloaded += 1
                                    _ += 1

                    #Logger.Info(f'Found {_} files')

                else:
                    #Logger.Error(f'No response or bad status ({StatusCode})')
                    pass
                    
            except Exception:
                #Logger.Error(f'Error processing page: {e}')
                pass

        if self.GlobalLimit <= 0:
            #Logger.Info('Global limit reached')
            pass
        if self.CreatorLimit <= 0:
            #Logger.Info(f'Creator limit reached for {self.Name}')
            pass

        await self.Client.aclose()
        return self.GlobalLimit, self.Result
    
def CheckForDuplicateIds():
    Logger.Info("Checking For Duplicate IDs...")
    
    def FindDuplicates(Items):
        Seen = {}
        Duplicates = []
        for Item in Items:
            NormalizedItem = Item.lower()
            if NormalizedItem in Seen:
                Duplicates.append(Item)
            else:
                Seen[NormalizedItem] = True
        return Duplicates

    for Platform in Config['directory_names'].keys():
        Duplicates = FindDuplicates(Config[Platform]['ids'])
        if Duplicates:
            Logger.Warning(f"[{Config['platform_names'][Platform]}] Found {len(Duplicates)} duplicate IDs:")
            for Duplicate in Duplicates:
                Logger.Warning(f"∙ {Duplicate}")

Screen = rf'''

 __   __     ______     ______     ______   ______     __   __     ______    
/\ "-.\ \   /\  ___\   /\  __ \   /\  ___\ /\  __ \   /\ "-.\ \   /\  ___\   
\ \ \-.  \  \ \  __\   \ \ \/\ \  \ \  __\ \ \  __ \  \ \ \-.  \  \ \___  \  
 \ \_\\"\_\  \ \_____\  \ \_____\  \ \_\    \ \_\ \_\  \ \_\\"\_\  \/\_____\ 
  \/_/ \/_/   \/_____/   \/_____/   \/_/     \/_/\/_/   \/_/ \/_/   \/_____/
                                                                                      
  [bold cyan]Rule34[/bold cyan] | [cornflower_blue]OnlyFans[/cornflower_blue] | [dodger_blue2]Fansly[/dodger_blue2] | [salmon1]Patreon[/salmon1] | [dark_cyan]SubscribeStar[/dark_cyan] | [deep_sky_blue4]E621[/deep_sky_blue4] | [sky_blue1]Fanbox[/sky_blue1] | [hot_pink]Gumroad[/hot_pink]
  by [light_coral]https://github.com/Hyphonic[/light_coral] | [white]Version {Config['version']}[/white]


'''

async def Main():
    Console(force_terminal=True).print(Screen)
    try:
        InitialGlobalLimit = Config['global_limit']

        CheckForDuplicateIds()

        # Set creator_limit to 0 if platform not in enabled_platforms
        for Platform in Config['directory_names'].keys():
            if not Config['enabled_platforms'][Platform]:
                Config[Platform]['creator_limit'] = 0

        await FavoriteFetcher.Create('coomer')  # Fetch favorites from Coomer
        await FavoriteFetcher.Create('kemono')  # Fetch favorites from Kemono

        for Platform in Config['directory_names'].keys():
            if Platform in ['rule34', 'e621']:
                for Creator in Config[Platform]['ids']:
                    Config[Platform]['directory_names'].append(f'{Config["directory_names"][Platform]}/{Creator.capitalize()}')
                    Config[Platform]['names'].append(Creator.capitalize())
            else:
                for Creator in Config[Platform]['names']:
                    Config[Platform]['directory_names'].append(f'{Config["directory_names"][Platform]}/{Creator.capitalize()}')

        for Platform in Config['directory_names'].keys():
            Logger.Debug(f'∙ Loaded {len(Config[Platform]["ids"])} {Config["platform_names"][Platform]} creators')
        Logger.Info(f'Loaded {sum([len(Config[Platform]["ids"]) for Platform in Config["directory_names"].keys() if Config[Platform]["creator_limit"] > 0])} creators')

        Logger.Debug('∙ Starting in dry-run mode') if Config.get('dry_run', False) else None

        # Create progress for each enabled platform
        ProgressColumns = [
            TextColumn("[cyan]{task.fields[platform]}"),
            BarColumn(bar_width=None),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("[blue]{task.fields[creator]}"),
            TextColumn("•"),
            MofNCompleteColumn(),
            TextColumn("•"),
            TextColumn("[yellow]{task.fields[global_progress]}/{task.fields[global_limit]}"),
            TimeRemainingColumn(),
        ]

        Collection = []

        for Platform in Config['directory_names'].keys():
            if Config[Platform]['creator_limit'] > 0:
                if Config['platform_limit_debug'] > 0:
                    Config[Platform]['ids'] = Config[Platform]['ids'][:Config['platform_limit_debug']]
                    Config[Platform]['names'] = Config[Platform]['names'][:Config['platform_limit_debug']]
                    Config[Platform]['directory_names'] = Config[Platform]['directory_names'][:Config['platform_limit_debug']]
            
            Config[Platform]['names'] = [Name.capitalize() for Name in Config[Platform]['names']]

        with Progress(*ProgressColumns, console=Console(force_terminal=True), auto_refresh=False, expand=True) as ProgressBar:
            TotalCreators = sum([
                len(Config[Platform]['ids']) 
                for Platform in Config['directory_names'].keys() 
                if Config[Platform]['creator_limit'] > 0
            ])
            
            # Single task that we'll update with current platform/creator
            Task = ProgressBar.add_task(
                "Downloading",
                total=TotalCreators,
                platform="Starting...",
                creator="Initializing...",
                global_progress=0,
                global_limit=Config['global_limit']
            )

            Tasks = []
            for Platform in Config['directory_names'].keys():
                if Config[Platform]['creator_limit'] > 0:
                    Tuple = list(zip(
                        Config[Platform]['ids'],
                        Config[Platform]['names'],
                        Config[Platform]['directory_names']
                    ))

                    for Data in Tuple:
                        Id, Name, DirectoryName = Data
                        # Update task fields instead of logging
                        ProgressBar.update(
                            Task,
                            advance=1,
                            platform=Config['platform_names'][Platform],
                            creator=Name.capitalize(),
                            global_progress=InitialGlobalLimit - Config['global_limit']  # Calculate remaining from current global limit
                        )
                        
                        FetcherInstance = Fetcher(
                            Platform=Platform,
                            Id=Id,
                            Name=Name,
                            DirectoryName=DirectoryName,
                            CachedHashes=Config['cached_hashes'],
                            CreatorLimit=Config[Platform]['creator_limit'],
                            GlobalLimit=Config['global_limit']
                        )
                        Tasks.append(FetcherInstance.Scrape())
                        ProgressBar.refresh()
        
        Results = await asyncio.gather(*Tasks)
        Collection.extend(Results)

        Results = await asyncio.gather(*Tasks)
        Collection.extend(Results)

        if Collection:
            AllFiles = []
            for Result in Collection:
                if Result and isinstance(Result, tuple) and len(Result) == 2:
                    GlobalLimit, ResultData = Result
                    for Platform in ResultData:
                        for Creator in ResultData[Platform]:
                            Files = ResultData[Platform][Creator]
                            for FileData in Files:
                                if isinstance(FileData, list) and len(FileData) == 3:
                                    AllFiles.append((FileData, Platform, Creator))
                        
            if AllFiles:
                Downloader = AsyncDownloadManager(AllFiles)
                Result = await Downloader.Start()
                if not Result:
                    Logger.Debug("∙ Hashes have been saved before exiting.")
                    return

    except KeyboardInterrupt:
        Logger.Warning("Interrupted by user")
    except Exception as e:
        Logger.Error(f"Error: {e}")
        Logger.Debug("∙ Hashes have been saved before exiting.")

if __name__ == '__main__':
    asyncio.run(Main())