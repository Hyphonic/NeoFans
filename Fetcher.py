# Main Imports - pip install -r requirements.txt
# Rclone Installation - https://rclone.org/install/
# Rclone Configuration - https://rclone.org/docs/
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError, retry_if_exception_type, wait_random
from rclone_python.utils import RcloneException
from rclone_python import rclone
from dotenv import load_dotenv
import aiofiles.os
import aiofiles
import asyncio
import aiohttp

# Default Imports
from dataclasses import dataclass
from typing import Union, Tuple
from asyncio import Queue
from pathlib import Path
import shutil
import random
import math
import sys
import os
import gc
import re

# Logging
from rich.console import Console as RichConsole
from rich.traceback import install as Install
from rich.highlighter import RegexHighlighter
from rich.logging import RichHandler
from rich.theme import Theme
import logging

# Config
LowDiskSpaceThreshold = max(5e+9, shutil.disk_usage('.').free * 0.1)
SemaphoreLimit = 16
QueueThresholds = [0.4, 0.8]
QueueLimit = 500
PageOffset = 50
StartingPage = 0
ChunkSize = 3e+6
TempDir = Path('Data/Temp')
FinalDir = Path('Data/Files')

UseRclone = False
Transfers = 12
MultiThreadStreams = 3
InitialFreeSpace = shutil.disk_usage('.').free
UploadThreshold = InitialFreeSpace * 0.1

rclone.set_log_level('ERROR')

TimeoutConfig = aiohttp.ClientTimeout(
    total=300,
    connect=30,
    sock_read=60
)

# Logging Configuration
def GradientColor(Start, End, Steps):
    return [
        f'#{int(Start[1:3], 16) + int((int(End[1:3], 16) - int(Start[1:3], 16)) / Steps * Step):02X}'
        f'{int(Start[3:5], 16) + int((int(End[3:5], 16) - int(Start[3:5], 16)) / Steps * Step):02X}'
        f'{int(Start[5:], 16) + int((int(End[5:], 16) - int(Start[5:], 16)) / Steps * Step):02X}'
        for Step in range(Steps)
    ]

def GenerateHighlightPatterns(Type, MaxValue, Format):
    return [re.compile(Format.format(Value=Value, Max=MaxValue)) for Value in range(MaxValue + 1)]

class DownloadHighlighter(RegexHighlighter):
    base_style = 'downloader.'
    highlights = [
        r'(?P<hash>[a-f0-9]{30})',
        r'(?P<status>Downloaded|Skipping|Failed To Move)',
        r'(?P<time>[\d.]+s)',
        r'(?P<filesize>[\d.]+ [KMGT]B)',
        r'\[(?P<error>error|Error)\]',
        r'\[(?P<warning>warning|Warning)\]',
        r'\[(?P<info>info|Info)\]',
    ]
    
    highlights.extend([rf'\[(?P<queue_{Queue}>{Queue}/{QueueLimit})\]' for Queue in range(QueueLimit + 1)])
    highlights.extend([rf'(?P<percent_{Percent}>{Percent}%)' for Percent in range(101)])

ThemeDict = {
    'log.time': 'bright_black',
    'logging.level.info': '#A0D6B4',
    'logging.level.warning': '#F5D7A3',
    'logging.level.error': '#F5A3A3',
    'downloader.filesize': '#A3D1F5',
    'downloader.hash': '#C8A3F5',
    'downloader.status': '#A0D6B4',
    'downloader.time': '#F5D7A3',
    'downloader.error': '#F5A3A3',
    'downloader.warning': '#F5D7A3',
    'downloader.info': '#A0D6B4'
}

def SetupThemeColors():
    QueueColors = GradientColor('#F5A3A3', '#A0D6B4', QueueLimit + 1)
    PercentColors = GradientColor('#A0D6B4', '#F5A3A3', 101)
    
    for Count, Color in enumerate(QueueColors):
        ThemeDict[f'downloader.queue_{Count}'] = Color
    
    for Count, Color in enumerate(PercentColors):
        ThemeDict[f'downloader.percent_{Count}'] = Color
    
    return Theme(ThemeDict)

def InitLogging():
    CustomTheme = SetupThemeColors()
    Console = RichConsole(theme=CustomTheme, force_terminal=True, log_path=False, 
                         highlighter=DownloadHighlighter(), color_system='truecolor')
    
    ConsoleHandler = RichHandler(markup=True, rich_tracebacks=True, show_time=True, 
                                console=Console, show_path=False, omit_repeated_times=True,
                                highlighter=DownloadHighlighter())
    
    ConsoleHandler.setFormatter(logging.Formatter('%(message)s', datefmt='[%H:%M:%S]'))
    
    logging.basicConfig(level=logging.INFO, handlers=[ConsoleHandler], force=True)
    
    Log = logging.getLogger('rich')
    Log.handlers.clear()
    Log.addHandler(ConsoleHandler)
    Log.propagate = False
    
    return Console, Log

def ErrorLogger(Error):
    if isinstance(Error, LowDiskSpace):
        Log.warning(f'Low disk space: {Error}')
    else:
        Console.print_exception(max_frames=1, width=Console.width or 120)

Console, Log = InitLogging()
Install()
load_dotenv()

RetryConfig = dict(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2),
    retry_error_cls=RetryError,
    retry=(
        retry_if_exception_type(aiohttp.ClientError) |
        retry_if_exception_type(aiohttp.ServerTimeoutError) |
        retry_if_exception_type(asyncio.TimeoutError)
    )
)

@dataclass
class FileData:
    ID: Union[str, int]
    Name: str
    Url: str
    Path: Path
    Hash: Union[str, int, Tuple[str, int]]
    Extension: str

@dataclass
class CreatorData:
    ID: Union[str, int]
    Name: str
    Platform: str
    Service: str

class LowDiskSpace(Exception):
    pass

Log.info('Rclone Is Installed' if rclone.is_installed() else 'Rclone Is Not Installed. Transfers Will Not Work!')

# Fetcher Class
class Fetcher:
    def __init__(self, Session: aiohttp.ClientSession, Log: logging.Logger, ErrorLogger: logging.Logger, DownloadQueue: Queue) -> None:
        self.Log = Log
        self.ErrorLogger = ErrorLogger
        self.Session = Session
        self.TotalFiles = 0
        self.DownloadQueue = DownloadQueue
        self.Stopped = False
        self.Hashes = set()
        self.Data = {
            'coomer':
                {
                    'BaseUrl': 'https://coomer.su/api/v1',
                    'FileUrl': 'https://coomer.su',
                    'Session': os.getenv('COOMER_SESS'),
                    'Services': ['onlyfans', 'fansly'],
                    'Creators': {
                        'onlyfans': [],
                        'fansly': []
                    },
                    'Directory': {
                        'onlyfans': '🌀 OnlyFans',
                        'fansly': '🔒 Fansly'
                    },
                    'Posts':
                        {
                            'onlyfans': [],
                            'fansly': []
                        }
                },
            'kemono':
                {
                    'BaseUrl': 'https://kemono.su/api/v1',
                    'FileUrl': 'https://kemono.su',
                    'Session': os.getenv('KEMONO_SESS'),
                    'Services': ['patreon', 'subscribestar', 'gumroad', 'fanbox'],
                    'Creators': {
                        'patreon': [],
                        'subscribestar': [],
                        'gumroad': [],
                        'fanbox': []
                    },
                    'Directory': {
                        'patreon': '🅿️ Patreon',
                        'subscribestar': '⭐ SubscribeStar',
                        'gumroad': '🍬 Gumroad',
                        'fanbox': '📦 Fanbox'
                    },
                    'Posts': {
                        'patreon': [],
                        'subscribestar': [],
                        'gumroad': [],
                        'fanbox': []
                    }
                }
            }

    async def CreateDirectories(self) -> None:
        Directories = rclone.ls(f'{rclone.get_remotes()[-1]}')
        for Platform in self.Data:
            for Directory in self.Data[Platform]['Directory'].values():
                if Directory not in [Dir['Name'] for Dir in Directories]:
                    rclone.mkdir(f'{rclone.get_remotes()[-1]}{Directory}')
                    self.Log.info(f'Created Missing Directory {Directory} On Remote Storage')

    async def LookupHashes(self) -> None:
        HashLookupTasks = []
        ProcessedHashes = set()

        async def ProcessCreator(Directory: str, CreatorName: str) -> None:
            self.Log.info(f'Looking Up Hashes For {CreatorName} From {Directory}...')
            try:
                Files = rclone.ls(f'{rclone.get_remotes()[-1]}{Directory}/{CreatorName}')
                for File in [File['Name'] for File in Files]:
                    Hash = Path(File).stem
                    if len(Hash) >= 30:
                        ProcessedHashes.add(Hash[:30])
            except RcloneException as Error:
                self.ErrorLogger(Error)
                self.Log.warning(f'Failed To Lookup Hashes For {CreatorName}')

        for Platform in self.Data:
            for Directory in self.Data[Platform]['Directory'].values():
                try:
                    Creators = rclone.ls(f'{rclone.get_remotes()[-1]}{Directory}')
                    for Creator in Creators:
                        HashLookupTasks.append(
                            asyncio.create_task(
                                ProcessCreator(Directory, Creator['Name'])
                            )
                        )
                except RcloneException:
                    self.Log.warning(f'Failed To Lookup Hashes For {Directory}')

        if HashLookupTasks:
            self.Log.info(f'Looking Up Hashes From {len(HashLookupTasks)} Creators...')
            await asyncio.gather(*HashLookupTasks)
            self.Hashes = ProcessedHashes
            self.Log.info(f'Loaded {len(self.Hashes)} Valid Hashes From Remote Storage')

    async def Favorites(self) -> None:
        Counter = 0
        Tasks = []

        @retry(**RetryConfig)
        async def Fetch(Platform: str, BaseUrl: str) -> None:
            nonlocal Counter
            try:
                async with self.Session.get(
                    f'{BaseUrl}/account/favorites?type=artist',
                    cookies={'session': self.Data[Platform]['Session']},
                ) as Response:
                    if Response.status == 200:
                        Creators = await Response.json()
                        for Creator in Creators:
                            self.Data[Platform]['Creators'][Creator['service']].append(
                                CreatorData(
                                    ID=Creator['id'],
                                    Name=Creator['name'].title(),
                                    Platform=Platform,
                                    Service=Creator['service']
                                )
                            )
                            Counter += 1
            except Exception as Error:
                self.ErrorLogger(Error)
                self.Log.warning(f'Failed To Fetch Favorites From {Platform.capitalize()}')

        for Platform in self.Data:
            Tasks.append(Fetch(Platform, self.Data[Platform]['BaseUrl']))

        await asyncio.gather(*Tasks)
        self.Log.info(f'Fetched {Counter} Favorites')

    async def Posts(self, Creator: CreatorData) -> None:
        TotalCounter = 0
        NewCounter = 0
        SkippedCounter = 0
        Page = StartingPage
        QueueThreshold = self.DownloadQueue.maxsize * QueueThresholds[1]

        if self.Stopped:
            return

        @retry(**RetryConfig)
        async def Fetch(Creator: CreatorData) -> None:
            nonlocal TotalCounter, NewCounter, SkippedCounter, Page
            while not self.Stopped:
                if self.DownloadQueue.qsize() >= QueueThreshold:
                    self.Log.warning(f'Pausing Fetcher For {Creator.Name} - Queue At {self.DownloadQueue.qsize()}')
                    while self.DownloadQueue.qsize() > (QueueThreshold * QueueThresholds[0]):
                        await asyncio.sleep(1)
                    self.Log.warning(f'Resuming Fetcher For {Creator.Name} - Queue At {self.DownloadQueue.qsize()}')

                try:
                    async with self.Session.get(
                        f'{self.Data[Creator.Platform]["BaseUrl"]}/{Creator.Service}/user/{Creator.ID}/posts',
                        cookies={'session': self.Data[Creator.Platform]['Session']},
                        params={'o': Page * PageOffset}
                    ) as Response:
                        if Response.status == 200:
                            Posts = await Response.json()
                            if not Posts:
                                break

                            for Post in Posts:
                                if Post.get('file') and Post['file'].get('path'):
                                    FilePath = Path(Post['file']['path'])
                                    TotalCounter += 1

                                    if not any(str(FilePath.stem).startswith(Hash) for Hash in self.Hashes):
                                        FileInfo = FileData(
                                            ID=Creator.ID,
                                            Name=Creator.Name,
                                            Url=f'{self.Data[Creator.Platform]["FileUrl"]}{Post["file"]["path"]}',
                                            Path=Path(f'Data/{self.Data[Creator.Platform]["Directory"][Creator.Service]}/{Creator.Name}'),
                                            Hash=FilePath.stem,
                                            Extension=FilePath.suffix
                                        )
                                        self.Data[Creator.Platform]['Posts'][Creator.Service].append(FileInfo)
                                        if self.DownloadQueue.full():
                                            self.Log.warning(f'Download Queue Full ({self.DownloadQueue.qsize()})')
                                            break
                                        else:
                                            await self.DownloadQueue.put(FileInfo)
                                        NewCounter += 1
                                        self.TotalFiles += 1
                                    else:
                                        SkippedCounter += 1

                                for Attachment in Post.get('attachments', []):
                                    if Attachment.get('path'):
                                        FilePath = Path(Attachment['path'])
                                        TotalCounter += 1

                                        if not any(str(FilePath.stem).startswith(Hash) for Hash in self.Hashes):
                                            FileInfo = FileData(
                                                ID=Creator.ID,
                                                Name=Creator.Name,
                                                Url=f'{self.Data[Creator.Platform]["FileUrl"]}{Attachment["path"]}',
                                                Path=Path(f'Data/{self.Data[Creator.Platform]["Directory"][Creator.Service]}/{Creator.Name}'),
                                                Hash=FilePath.stem,
                                                Extension=FilePath.suffix
                                            )
                                            self.Data[Creator.Platform]['Posts'][Creator.Service].append(FileInfo)
                                            NewCounter += 1
                                            self.TotalFiles += 1
                                        else:
                                            SkippedCounter += 1
                            Page += 1
                except Exception as Error:
                    self.ErrorLogger(Error)
                    self.Log.warning(f'Failed To Fetch Posts From {Creator.Name}')
                    break

        await Fetch(Creator)
        if not self.Stopped:
            self.Log.info(f'Fetched {NewCounter} New Posts From {Creator.Name} After {Page} Pages (Skipped: {SkippedCounter})')

# Downloader Class
class Downloader:
    def __init__(self, Session: aiohttp.ClientSession, Log: logging.Logger, ErrorLogger: logging.Logger, Fetcher: Fetcher) -> None:
        self.Log = Log
        self.ErrorLogger = ErrorLogger
        self.Session = Session
        self.Semaphore = asyncio.Semaphore(SemaphoreLimit)
        self.TotalFiles = 0
        self.Stopped = False
        self.Fetcher = Fetcher
        self.Hashes = Fetcher.Hashes
        self.InitialFreeSpace = shutil.disk_usage('.').free

    @retry(**RetryConfig)
    async def FetchFile(self, Url: str, OutPath: Path) -> int:
        TotalSize = 0
        async with self.Session.get(Url) as Response:
            if Response.status != 200:
                raise aiohttp.ClientError(f'Status: {Response.status}')

            async with aiofiles.open(OutPath, 'wb') as F:
                async for Chunk in Response.content.iter_chunked(ChunkSize):
                    if self.Stopped:
                        return 0
                    await F.write(Chunk)
                    TotalSize += len(Chunk)
        return TotalSize

    async def Download(self, File: FileData) -> None:
        if self.Stopped:
            return

        if random.random() < 0.1:
            gc.collect()

        async with self.Semaphore:
            try:
                if str(File.Hash) in self.Hashes:
                    SpacePercentage = await self.CalculateSpacePercentage()
                    QueueStatus = f'[{self.Fetcher.DownloadQueue.qsize()}/{QueueLimit}]'
                    self.Log.warning(
                        f'{QueueStatus} ({SpacePercentage}) Skipping {File.Hash[:30]}... '
                    ) if not self.Stopped else None
                    return

                if shutil.disk_usage('.').free < LowDiskSpaceThreshold:
                    self.Log.warning('Low Disk Space!') if not self.Stopped else None
                    self.Stopped = True
                    self.Fetcher.Stopped = True
                    raise LowDiskSpace(f'Available Disk Space Below {await self.CalculateSpacePercentage()}')

                StartTime = asyncio.get_event_loop().time()
                TempPath = TempDir / File.Path.relative_to('Data')
                FinalPath = FinalDir / File.Path.relative_to('Data')
                os.makedirs(TempPath, exist_ok=True)
                os.makedirs(FinalPath, exist_ok=True)

                FileSize = await self.FetchFile(
                    File.Url,
                    TempPath / f'{File.Hash[:30]}{File.Extension}'
                )

                if FileSize > 0:
                    await aiofiles.os.makedirs(FinalPath, exist_ok=True)
                    try:
                        await aiofiles.os.rename(
                            TempPath / f'{File.Hash[:30]}{File.Extension}',
                            FinalPath / f'{File.Hash[:30]}{File.Extension}'
                        )
                    except FileNotFoundError:
                        SpacePercentage = await self.CalculateSpacePercentage()
                        QueueStatus = f'[{self.Fetcher.DownloadQueue.qsize()}/{QueueLimit}]'
                        self.Log.warning(f'{QueueStatus} ({SpacePercentage}) Failed To Move {File.Hash[:30]}...')
                        pass
                    self.Hashes.add(str(File.Hash))
                    ElapsedTime = asyncio.get_event_loop().time() - StartTime
                    SpacePercentage = await self.CalculateSpacePercentage()
                    QueueStatus = f'[{self.Fetcher.DownloadQueue.qsize()}/{QueueLimit}]'
                    self.Log.info(
                        f'{QueueStatus} ({SpacePercentage}) Downloaded {File.Hash[:30]}... '
                        f'({await Humanize(FileSize)} in {ElapsedTime:.1f}s)'
                    ) if not self.Stopped else None

            except RetryError as Error:
                if not self.Stopped:
                    self.ErrorLogger(Error)
                    SpacePercentage = await self.CalculateSpacePercentage()
                    QueueStatus = f'[{self.Fetcher.DownloadQueue.qsize()}/{QueueLimit}]'
                    self.Log.warning(f'{QueueStatus} ({SpacePercentage}) Retry Limit Exceeded For {File.Hash[:30]}...')
                    pass
            except Exception as Error:
                if Error is any([
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    aiohttp.ServerTimeoutError,
                    BlockingIOError,
                    RuntimeError,
                    aiohttp.ClientConnectionError,
                    aiohttp.ClientOSError
                    ]):
                    pass
                else:
                    if not self.Stopped:
                        self.ErrorLogger(Error)
                        SpacePercentage = await self.CalculateSpacePercentage()
                        QueueStatus = f'[{self.Fetcher.DownloadQueue.qsize()}/{QueueLimit}]'
                        self.Log.warning(f'{QueueStatus} ({SpacePercentage}) Failed To Download {File.Hash[:30]}... ')

    async def CalculateSpacePercentage(self) -> str:
        CurrentFreeSpace = shutil.disk_usage('.').free
        UsedSpace = self.InitialFreeSpace - CurrentFreeSpace
        if UsedSpace <= 0:
            return '0.00%'
        Percentage = min(100, (UsedSpace / (self.InitialFreeSpace - LowDiskSpaceThreshold)) * 100)
        return f'{Percentage:.2f}%'

async def Humanize(Bytes: int) -> str:
    for Unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if Bytes < 1024.0:
            break
        Bytes /= 1024.0
    return f'{Bytes:.2f} {Unit}'

async def CalculateTransfers(FileCount, MinTransfers=4, MaxTransfers=32, MinFiles=100, MaxFiles=50000):
    return max(MinTransfers, min(MaxTransfers, round(MinTransfers + (MaxTransfers - MinTransfers) * ((math.log(FileCount) - math.log(MinFiles)) / (math.log(MaxFiles) - math.log(MinFiles))))))

if __name__ == '__main__':
    async def Main() -> None:
        async def MoveToRemote():
            Log.info('Starting Background Move Task')
            while True:
                try:
                    if not Path(FinalDir).exists():
                        await asyncio.sleep(30)
                        continue
                        
                    DirSize = sum(f.stat().st_size for f in Path(FinalDir).rglob('*') if f.is_file())
                    FileCount = sum(1 for _ in Path(FinalDir).rglob('*') if _.is_file())
                    
                    if DirSize >= UploadThreshold and FileCount > 0:
                        HumanSize = await Humanize(DirSize)
                        Log.info(f'Moving {HumanSize} ({FileCount} files) To Remote Storage')
                        
                        # Dynamic transfers based on file count
                        DynamicTransfers = min(32, max(4, FileCount // 10))
                        
                        rclone.move(
                            str(FinalDir), 
                            rclone.get_remotes()[-1], 
                            show_progress=False, 
                            args=[
                                '--transfers', str(DynamicTransfers),
                                '--multi-thread-streams', str(MultiThreadStreams),
                                '--checkers', str(min(32, DynamicTransfers * 2)),
                                '--stats-one-line',
                                '--stats', '1s'
                            ]
                        )
                        Log.info('Move Completed')
                    await asyncio.sleep(30)
                except RcloneException as Error:
                    ErrorLogger(Error)
                    Log.warning('Rclone Move Failed - Retrying In 5 Minutes')
                    await asyncio.sleep(300)
                except FileNotFoundError:
                    await asyncio.sleep(30)
                except Exception as Error:
                    ErrorLogger(Error)
                    Log.error('Unexpected Error In Move Task')
                    await asyncio.sleep(60)

        Log.info(f'Low Disk Space Threshold: {await Humanize(LowDiskSpaceThreshold)}')
        DownloadQueue = Queue(maxsize=QueueLimit)

        async def ProcessDownloads(Download: Downloader):
            while True:
                File = await DownloadQueue.get()
                try:
                    await Download.Download(File)
                except Exception as Error:
                    ErrorLogger(Error)
                finally:
                    DownloadQueue.task_done()

        async with aiohttp.ClientSession(timeout=TimeoutConfig) as Session:
            Fetch = Fetcher(Session, Log, ErrorLogger, DownloadQueue)
            Download = Downloader(Session, Log, ErrorLogger, Fetch)

            DownloadTasks = [
                asyncio.create_task(ProcessDownloads(Download))
                for _ in range(SemaphoreLimit)
            ]

            MoverTask = asyncio.create_task(MoveToRemote()) if UseRclone else None
            Log.info('Creating Directories...')
            await Fetch.CreateDirectories()
            Log.info('Looking Up Hashes...')
            await Fetch.LookupHashes()
            Log.info('Fetching Favorites...')
            await Fetch.Favorites()

            AllCreators = []
            for Platform in Fetch.Data:
                for Service in Fetch.Data[Platform]['Creators']:
                    AllCreators.extend(Fetch.Data[Platform]['Creators'][Service])

            random.shuffle(AllCreators)

            for Creator in AllCreators:
                await Fetch.Posts(Creator)

            Fetch.Log.info(f'Fetched {Fetch.TotalFiles} Files')
            Download.TotalFiles = Fetch.TotalFiles

            await DownloadQueue.join()
            for Task in DownloadTasks:
                Task.cancel()
            await asyncio.gather(*DownloadTasks, return_exceptions=True)
            if MoverTask:
                MoverTask.cancel()

            FileCount = sum(1 for _ in Path(FinalDir).rglob('*') if _.is_file())
            OptimalTransfers = await CalculateTransfers(FileCount)

            async with aiofiles.open('Data/Transfers.txt', 'w') as F:
                await F.write(str(OptimalTransfers))
            Log.info(f'Optimal Rclone Transfers: {OptimalTransfers} (Based on {FileCount} files)')

    try:
        asyncio.run(Main())
    except KeyboardInterrupt:
        Log.info('Exiting...')
        for File in TempDir.iterdir():
            try:
                Log.info(f'Deleting {File.name}')
                File.unlink()
            except Exception as Error:
                ErrorLogger(Error)
        sys.exit(0)
    except LowDiskSpace as Error:
        Log.warning(Error)
        sys.exit(0)
    except RcloneException:
        pass
    except Exception as Error:
        if not isinstance(Error, asyncio.CancelledError):
            ErrorLogger(Error)
        sys.exit(1)