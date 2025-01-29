import aiofiles.os
from rich.console import Console
from dotenv import load_dotenv
from typing import Dict
import urllib.parse
import aiofiles
import asyncio
import httpx
import json
import os

from rich.progress import Progress, BarColumn, TimeElapsedColumn

load_dotenv()

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
            self.Console.log(f'[bold blue]DEBUG:   [/bold blue] {Message}')

    def Info(self, Message):
        if LOG_LEVEL <= self.LogLevels['INFO']:
            self.Console.log(f'[bold green]INFO:    [/bold green] {Message}')

    def Warning(self, Message):
        if LOG_LEVEL <= self.LogLevels['WARNING']:
            self.Console.log(f'[bold yellow]WARNING: [/bold yellow] {Message}')

    def Error(self, Message):
        if LOG_LEVEL <= self.LogLevels['ERROR']:
            self.Console.log(f'[bold red]ERROR:   [/bold red] {Message}')

    def Critical(self, Message):
        if LOG_LEVEL <= self.LogLevels['CRITICAL']:
            self.Console.log(f'[bold magenta]CRITICAL:[/bold magenta] {Message}')

Logger = RichLogger(__name__)

async def ReadConfig():
    async with aiofiles.open('config.json', 'r', encoding='utf-8') as f:
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

class AsyncDownloader:
    def __init__(self, FileData: tuple, Platform: str, Creator: str):
        self.Hash = FileData[0]
        self.Url = FileData[1]
        self.Path = FileData[2]
        self.Platform = Platform
        self.Creator = Creator
        self.Client = httpx.AsyncClient()

        self.FullPath = self.Path + self.Hash + os.path.splitext(self.Url)[1]
        self.PartialPath = f'{self.FullPath}.partial'

    async def Download(self):
        try:
            Response = await self.Client.get(self.Url)
            if Response.status_code == 200:
                # Download to .partial file first
                async with aiofiles.open(self.PartialPath, 'wb') as f:
                    await f.write(Response.content)
                
                try:
                    # Rename to final filename if download successful
                    if os.path.exists(self.PartialPath):
                        os.rename(self.PartialPath, self.FullPath)
                        return True
                except Exception as e:
                    Logger.Error(f'Failed to rename {self.PartialPath}: {e}')
                    # Clean up partial file on rename failure
                    if os.path.exists(self.PartialPath):
                        os.remove(self.PartialPath)
                    return False
            else:
                return False

        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError):
            return False
        except Exception as e:
            if not isinstance(e, (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError)):
                Console(force_terminal=True).print_exception(max_frames=1)
            return False
        finally:
            # Clean up partial file on any other failure
            if os.path.exists(self.PartialPath) and not os.path.exists(self.FullPath):
                os.remove(self.PartialPath)
            await self.Client.aclose()
    
    async def Size(self):
        try:
            Response = await self.Client.head(self.Url)
            if Response.status_code == 200:
                return int(Response.headers['content-length'])
            else:
                return 0
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError):
            return 0
        except Exception as e:
            if not isinstance(e, (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError)):
                Console(force_terminal=True).print_exception(max_frames=1)
            return 0

class HashManager:
    '''Handles loading and saving cached hashes.'''
    def __init__(self, CacheFile: str = 'cached_hashes.json'):
        self.CacheFile = CacheFile
        self.CachedHashes = {}

    async def LoadCache(self):
        '''Load cached hashes from the cache file.'''
        try:
            async with aiofiles.open(self.CacheFile, 'r') as f:
                Content = await f.read()
                self.CachedHashes = json.loads(Content)
                TotalHashes = sum(len(Hashes) for Platform in self.CachedHashes.values() 
                                for Hashes in Platform.values())
                Logger.Debug(f'∙ Loaded {TotalHashes} cached hashes')
        except (FileNotFoundError, json.JSONDecodeError):
            self.CachedHashes = {}
            Logger.Debug('∙ No existing cache found, starting fresh')

    async def SaveHashes(self, NewHashes: Dict[str, Dict[str, list[str]]]):
        '''Save new hashes to the cache file while preserving existing ones.'''
        try:
            Logger.Debug('Saving new hashes for platforms:')
            
            for Platform, CreatorData in NewHashes.items():
                Logger.Debug(f'∙ {Platform}: {len(CreatorData)} creators')
                for Creator, Hashes in CreatorData.items():
                    Logger.Debug(f'∙ {Creator}: {len(Hashes)} hashes')
                if Platform not in self.CachedHashes:
                    self.CachedHashes[Platform] = {}
                    
                for Creator, Hashes in CreatorData.items():
                    if Creator not in self.CachedHashes[Platform]:
                        self.CachedHashes[Platform][Creator] = []
                    self.CachedHashes[Platform][Creator].extend(Hashes)
                    Logger.Debug(f'Added {len(Hashes)} hashes for {Platform}/{Creator}')
            
            async with aiofiles.open(self.CacheFile, 'w') as f:
                await f.write(json.dumps(self.CachedHashes, indent=4))
            
        except Exception as e:
            Logger.Error(f'Failed to save hashes: {str(e)}')
            Console(force_terminal=True).print_exception(max_frames=1)
    
    async def HasHash(self, Platform: str, Creator: str, Hash: str) -> bool:
        '''Check if a hash is already cached.'''
        return Hash in self.CachedHashes.get(Platform, {}).get(Creator, [])

class Fetcher:
    def __init__(self, Platform, Id, Name, DirectoryName, HashManager, CreatorLimit, GlobalLimit):
        self.Page = 0
        self.Client = httpx.AsyncClient()
        
        # Rest of init remains same
        self.Platform = Platform
        self.Id = Id
        self.Name = Name
        self.DirectoryName = DirectoryName
        self.CreatorLimit = CreatorLimit
        self.GlobalLimit = GlobalLimit
        self.Result = {self.Platform: {self.Id: []}}
        self.FilesDownloaded = 0

        self.HashManager = HashManager
        self.LastPage = 0  # Track last visited page

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

    def ExtractHash(self, Url):  # Extract hash from URL / <hash> .png
        if not Url:
            return None
        # Extract the filename from the URL and split on last dot
        Filename = Url.split('/')[-1]
        return Filename.rsplit('.', 1)[0]

    async def FetchUrl(self, Url: str, Params: Dict = None) -> Dict:
        try:
            Response = await self.Client.get(Url, params=Params, timeout=30.0)
            if Response.status_code == 200:
                return Response.json(), Response.status_code
            return None, Response.status_code
        except Exception:
            return None, None
        except httpx.TimeoutException:
            return None, None

    async def Scrape(self):
        #Logger.Debug(f'\n∙ Scraping {self.Platform} for {self.Name}')
        #Logger.Debug(f'∙ Creator Limit: {self.CreatorLimit} | Global Limit: {self.GlobalLimit}\n')
        if self.Platform == 'rule34':
            BaseParams = dict(urllib.parse.parse_qsl(self.Params))
            while self.GlobalLimit > 0 and self.CreatorLimit > 0:
                BaseParams['pid'] = self.Page
                self.LastPage = self.Page + 1  # Rule34 uses 0-based indexing
                ReEncodedParams = urllib.parse.urlencode(BaseParams, safe='+')
                Response, StatusCode = await self.FetchUrl('https://api.rule34.xxx/index.php', ReEncodedParams)
                
                try:
                    _ = 0
                    if Response:
                        #Logger.Debug(f'∙ Got {len(Response)} posts for {self.Platform}/{self.Name}')
                        Data = Response
                        if not Data or (isinstance(Data, list) and len(Data) == 0):
                            #Logger.Error(f'No data at page {self.Page+1}')
                            break
                        
                        for Post in Data:
                            if self.GlobalLimit > 0 and self.CreatorLimit > 0:
                                FileUrl = Post.get('file_url')
                                FileHash = self.ExtractHash(FileUrl)

                                if FileHash and await self.HashManager.HasHash(self.Platform, self.Id, FileHash):
                                    #Logger.Debug(f'∙ Skipping {FileHash} as it is already cached')
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
                BaseParams['page'] = self.Page + 1
                self.LastPage = self.Page + 1  # e621 uses 1-based indexing
                ReEncodedParams = urllib.parse.urlencode(BaseParams, safe='+')
                Response, StatusCode = await self.FetchUrl('https://e621.net/posts.json', ReEncodedParams)
                
                try:
                    if Response and 'posts' in Response:
                        #Logger.Debug(f'∙ Got {len(Response)} posts for {self.Platform}/{self.Name}')
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
                                if FileHash and await self.HashManager.HasHash(self.Platform, self.Id, FileHash):
                                    #Logger.Debug(f'∙ Skipping {FileHash} as it is already cached')
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
            while self.GlobalLimit > 0 and self.CreatorLimit > 0:
                self.LastPage = self.Page  # These platforms use offset-based pagination
                Response, StatusCode = await self.FetchUrl(f'https://{Hoster}.su/api/v1/{self.Platform}/user/{self.Id}?o={self.Page}')
                
                try:
                    if Response and isinstance(Response, list):  # Change here - response is a list     
                        #Logger.Debug(f'∙ Got {len(Response)} posts for {self.Platform}/{self.Name}')
                        _ = 0
                        for Post in Response:
                            if self.GlobalLimit > 0 and self.CreatorLimit > 0:
                                # Handle attachments
                                for Attachment in Post.get('attachments', []):
                                    if self.GlobalLimit <= 0 or self.CreatorLimit <= 0:
                                        break
                                        
                                    FileUrl = f'https://{Hoster}.su{Attachment.get('path')}'
                                    FileHash = self.ExtractHash(FileUrl)
                                    
                                    if FileHash and await self.HashManager.HasHash(self.Platform, self.Id, FileHash):
                                        #Logger.Debug(f'∙ Skipping {FileHash} as it is already cached')
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
                                    FileUrl = f'https://{Hoster}.su{File.get('path')}'
                                    FileHash = self.ExtractHash(FileUrl)
                                    
                                    if FileHash and await self.HashManager.HasHash(self.Platform, self.Id, FileHash):
                                        #Logger.Debug(f'∙ Skipping {FileHash} as it is already cached')
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
                        break

                    if _ < self.ParamsLimit:
                        #Logger.Info(f'Page {self.Page} → {_} files')
                        break

                    #Logger.Info(f'Page {self.Page} → {self.FilesDownloaded} files')
                        
                except Exception:
                    #Logger.Error(f'Error processing page: {e}')
                    break

                self.Page += 50

        if self.GlobalLimit <= 0:
            #Logger.Info('Global limit reached')
            pass
        if self.CreatorLimit <= 0:
            #Logger.Info(f'Creator limit reached for {self.Name}')
            pass

        await self.Client.aclose()
        return self.GlobalLimit, self.Result, self.LastPage

async def CreateDirectories(Directories):
    with Progress(
        '[progress.description]{task.description}',
        BarColumn(bar_width=None),
        '[progress.percentage]{task.percentage:>3.0f}%',
        '•',
        '{task.fields[creator]}',
        console=Console(force_terminal=True),
        auto_refresh=False
    ) as ProgressBar:
        MainTask = ProgressBar.add_task(
            '',
            total=len(Directories),
            creator=''
        )

        for Directory in Directories:
            try:
                os.makedirs(Directory, exist_ok=True)
                ProgressBar.update(
                    MainTask,
                    description='[blue]Creating Directories[/blue]',
                    advance=1,
                    creator=Directory,
                )
                ProgressBar.refresh()
            except Exception as e:
                Logger.Error(f'Failed to create directory {Directory}: {e}')
    
def CheckForDuplicateIds():
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
            Logger.Warning(f'[{Config['platform_names'][Platform]}] Found {len(Duplicates)} duplicate IDs:')
            for Duplicate in Duplicates:
                Logger.Warning(f'∙ {Duplicate}')

def HumanizeBytes(Bytes):
    if Bytes == 0:
        return '0 B'
    Sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    i = int((Bytes).bit_length() / 10)
    return f'{round(Bytes / (1 << (i * 10)), 2)} {Sizes[i]}'

Screen = rf'''

 __   __     ______     ______     ______   ______     __   __     ______    
/\ `-.\ \   /\  ___\   /\  __ \   /\  ___\ /\  __ \   /\ `-.\ \   /\  ___\   
\ \ \-.  \  \ \  __\   \ \ \/\ \  \ \  __\ \ \  __ \  \ \ \-.  \  \ \___  \  
 \ \_\\`\_\  \ \_____\  \ \_____\  \ \_\    \ \_\ \_\  \ \_\\`\_\  \/\_____\ 
  \/_/ \/_/   \/_____/   \/_____/   \/_/     \/_/\/_/   \/_/ \/_/   \/_____/
                                                                                      
  {' | '.join(Config['platform_names'][Platform] for Platform in Config['platform_names'].keys())}
  by [light_coral]https://github.com/Hyphonic[/light_coral] | [white]Version {Config['version']}[/white]


'''

############################################################
#                                                          #
#                           Main                           #
#                                                          #
############################################################

# Fix FavoriteFetcher usage
Manager = HashManager()  # Single instance to be reused

async def Main():
    Console(force_terminal=True).print(Screen)
    CheckForDuplicateIds()

    Logger.Debug('Fetching Creators:')

    # Initialize favorites properly
    await FavoriteFetcher.Create('coomer')
    await FavoriteFetcher.Create('kemono')

    for Platform in Config['directory_names'].keys():
        if Config[Platform]['creator_limit'] > 0:
            Logger.Info(f'∙ Loaded {len(Config[Platform]['ids'])} creators from {Config['platform_names'][Platform]}')
    
    Logger.Info(f'∙ Loaded {sum(len(Config[Platform]['ids']) for Platform in Config['directory_names'].keys())} creators in total')

    Logger.Debug('Loading Cached Hashes:')

    # Initialize hash manager once
    await Manager.LoadCache()

    # Set directory names

    for Platform in Config['directory_names'].keys():
        Config[Platform]['directory_names'] = [f'{Config['directory_names'][Platform]}/{Name}' for Name in Config[Platform]['names']]

    # Limit the number of creators to fetch
    for Platform in Config['directory_names'].keys():
        Config[Platform]['ids'] = Config[Platform]['ids'][:Config['platform_limit_debug']]
        Config[Platform]['names'] = Config[Platform]['names'][:Config['platform_limit_debug']]
        Config[Platform]['directory_names'] = Config[Platform]['directory_names'][:Config['platform_limit_debug']]
        #Logger.Debug(f'∙ {Config['platform_names'][Platform]}: {Config[Platform]['ids']}, {Config[Platform]['names']}, {Config[Platform]['directory_names']}')

    # Initialize progress tracking
    TotalCreators = sum(len(Config[Platform]['ids']) for Platform in Config['directory_names'].keys())
    CurrentCreator = 0
    TotalFilesFetched = 0
    InitialGlobalLimit = Config['global_limit']

    with Progress(
        '[progress.description]{task.description}',
        BarColumn(bar_width=None),
        '[progress.percentage]{task.percentage:>3.0f}%',
        '•',
        '{task.fields[creator]}',
        '•',
        '{task.fields[progress]}',
        '•',
        '{task.fields[files]}',
        '•',
        '{task.fields[page]}',  # Add page display
        TimeElapsedColumn(),
        console=Console(force_terminal=True),
        auto_refresh=False
    ) as ProgressBar:
        MainTask = ProgressBar.add_task(
            '',
            total=TotalCreators,
            creator='',
            progress='0/0',
            files='0/0',
            page='Page 0'
        )

        Results = []  # Add this line to store results
        for Platform in Config['directory_names'].keys():
            if Config[Platform]['creator_limit'] > 0:
                Tuple = tuple(zip(
                    Config[Platform]['ids'],
                    Config[Platform]['names'],
                    Config[Platform]['directory_names']
                ))

                for Data in Tuple:
                    Id, Name, DirectoryName = Data
                    CurrentCreator += 1
                    FetcherInstance = Fetcher(
                        Platform=Platform,
                        Id=Id, 
                        Name=Name,
                        DirectoryName=DirectoryName,
                        HashManager=Manager,  # Pass Manager instance
                        CreatorLimit=Config[Platform]['creator_limit'],
                        GlobalLimit=Config['global_limit']
                    )
                    
                    GlobalLimit, Result, LastPage = await FetcherInstance.Scrape()

                    # Format page display based on platform
                    PageDisplay = (
                        f'Page {LastPage}' if Platform in ['rule34', 'e621']
                        else f'Offset {LastPage}'
                    )
                    
                    # Calculate total files fetched for this creator
                    CreatorFiles = sum(len(files) for service in Result.values() 
                                    for creator_files in service.values() 
                                    for files in [creator_files])
                    TotalFilesFetched += CreatorFiles

                    ProgressBar.update(
                        MainTask,
                        description=f'[blue]{Config['platform_names'][Platform]}[/blue]',
                        advance=1,
                        creator=f'{Name}',
                        progress=f'{CurrentCreator}/{TotalCreators}',
                        files=f'{TotalFilesFetched}/{InitialGlobalLimit}',
                        page=PageDisplay
                    )
                    ProgressBar.refresh()

                    # Store result for final processing
                    Results.append((GlobalLimit, Result))

                    # Results structure:
                    # [
                    #     (GlobalLimit, {
                    #         'platform': {
                    #             'creator': [
                    #                 [hash, url, path]
                    #             ]
                    #         }
                    #     })
                    # ]
    
    # Process results
    AllFiles = []
    for GlobalLimit, Result in Results:
        if Result:
            for Platform in Result:
                for Creator in Result[Platform]:
                    AllFiles.extend([(FileData, Platform, Creator) 
                                    for FileData in Result[Platform][Creator]])
    
    Logger.Info(f'∙ Found {len(AllFiles)} new files to download')
    
    # Create directories
    Directories = [os.path.dirname(File[0][2]) for File in AllFiles]
    await CreateDirectories(list(set(Directories)))

    # Download files
    with Progress(
        '[progress.description]{task.description}',
        BarColumn(bar_width=None),
        '[progress.percentage]{task.percentage:>3.0f}%',
        '•',
        '{task.fields[creator]}',
        '•', 
        '{task.fields[progress]}',
        '•',
        '{task.fields[size]}',
        TimeElapsedColumn(),
        console=Console(force_terminal=True),
    ) as ProgressBar:
        MainTask = ProgressBar.add_task(
            "",
            total=len(AllFiles),
            creator="",
            progress="0/0",
            size="0B"
        )
        CompletedFiles = 0
        PlatformsHandled = []
        Semaphore = asyncio.Semaphore(Config['threads']['max_workers'])
        
        # Track successful downloads
        SuccessfulDownloads = {Platform: {Creator: [] for Creator in Config[Platform]['ids']} 
                             for Platform in Config['directory_names'].keys()}

        async def Worker(File):
            nonlocal CompletedFiles
            async with Semaphore:
                FileData, Platform, Creator = File
                if Platform not in PlatformsHandled:
                    PlatformsHandled.append(Platform)
                
                Downloader = AsyncDownloader(FileData, Platform, Creator)
                FileSize = await Downloader.Size()
                Success = await Downloader.Download()

                if Success:
                    # Store successful download
                    SuccessfulDownloads[Platform][Creator].append(FileData[0])
                
                CompletedFiles += 1
                Name = Config[Platform]['names'][Config[Platform]['ids'].index(Creator)]
                ProgressBar.update(
                    MainTask,
                    description=f'[blue]{Config['platform_names'][Platform]}[/blue]',
                    advance=1,
                    creator=f'{Name}',
                    progress=f'{CompletedFiles}/{len(AllFiles)}',
                    size=f'{HumanizeBytes(FileSize)}'
                )
                ProgressBar.refresh()
                
            
        Tasks = [Worker(File) for File in AllFiles]
        await asyncio.gather(*Tasks)

        # Save all hashes at once after downloads complete
        Logger.Debug("Successfully downloaded files:")
        for Platform in SuccessfulDownloads:
            for Creator, Hashes in SuccessfulDownloads[Platform].items():
                if Hashes:  # Only log/save non-empty hash lists
                    Logger.Debug(f"∙ {Platform}/{Creator}: {Hashes}")
                    await Manager.SaveHashes({Platform: {Creator: Hashes}})

if __name__ == '__main__':
    try:
        asyncio.run(Main())
    except KeyboardInterrupt:
        Logger.Warning('Program interrupted by user')
    except Exception:
        Console(force_terminal=True).print_exception(max_frames=1)
    finally:
        Logger.Info('Exiting program')