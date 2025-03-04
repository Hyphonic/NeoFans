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
import orjson

# Default Imports
from dataclasses import dataclass
from typing import Union, Tuple
from collections import deque
from platform import system
from asyncio import Queue
from pathlib import Path
import statistics
import shutil
import random
import math
import sys
import os
import gc
import re

# Special Imports For Linux Systems
if system() == 'Linux':
    import uvloop

# Logging
from rich.console import Console as RichConsole
from rich.traceback import install as Install
from rich.highlighter import RegexHighlighter
from rich.logging import RichHandler
from rich.theme import Theme
import logging

# Config
QueueThresholds = [.2, .8]
QueueLimit = 2500
PageOffset = 50
StartingPage = 0
ChunkSize = 3e+6
TempDir = Path('Data/Temp')
FinalDir = Path('Data/Files')
LowDiskSpaceThreshold = max(5e+9, shutil.disk_usage('.').free * 0.1)
rclone.set_log_level('ERROR')
TimeoutConfig = 300.0

AiohttpExceptions = [
    aiohttp.ClientError,
    aiohttp.ClientConnectionError, 
    aiohttp.ClientOSError,
    aiohttp.ServerConnectionError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ServerTimeoutError,
    aiohttp.ClientResponseError,
    aiohttp.ClientPayloadError,
    aiohttp.ClientHttpProxyError,
    aiohttp.WSServerHandshakeError,
    aiohttp.ContentTypeError,
    asyncio.TimeoutError
]

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
        r'(?P<status>Downloaded|Skipping|Failed To Move|Speed|Files|Total|Downloads)',
        r'(?P<time>[\d.]+s)',
        r'(?P<file_k>[\d.]+ KB)',
        r'(?P<file_m>[\d.]+ MB)',
        r'(?P<file_g>[\d.]+ GB)',
        r'(?P<file_t>[\d.]+ TB)',
        r'\[(?P<error>error|Error)\]',
        r'\[(?P<warning>warning|Warning)\]',
        r'\[(?P<info>info|Info)\]',
    ]

    highlights.extend([rf'\[(?P<queue_{Queue}>{Queue}/{QueueLimit})\]' for Queue in range(QueueLimit + 1)])

    for Percent in range(101):
        if Percent < 100:
            highlights.append(rf'(?P<percent_{Percent}>{Percent}\.\d{{2}}%)')
        else:
            highlights.append(r'(?P<percent_100>100\.00%)')

ThemeDict = {
    'log.time': 'bright_black',
    'logging.level.info': '#A0D6B4',
    'logging.level.warning': '#F5D7A3',
    'logging.level.error': '#F5A3A3',
    'downloader.file_k': '#B5E8C9',
    'downloader.file_m': '#D1C2E0',
    'downloader.file_g': '#F7D4BC',
    'downloader.file_t': '#B3D7EC',
    'downloader.hash': '#C8A3F5',
    'downloader.status': '#A0D6B4',
    'downloader.time': '#F5D7A3',
    'downloader.error': '#F5A3A3',
    'downloader.warning': '#F5D7A3',
    'downloader.info': '#A0D6B4',
}

def SetupThemeColors():
    QueueColors = GradientColor('#F5A3A3', '#A0D6B4', QueueLimit + 1)
    PercentColors = GradientColor('#A0D6B4', '#B3D7EC', 101)

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

    logging.getLogger('httpx').setLevel(logging.WARNING)

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
        retry_if_exception_type(aiohttp.ClientConnectionError) |
        retry_if_exception_type(aiohttp.ServerTimeoutError) |
        retry_if_exception_type(aiohttp.ClientPayloadError) |
        retry_if_exception_type(aiohttp.ServerDisconnectedError) |
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

    async def LookupHashes(self, HashLookupLimit=16) -> None:
        ProcessedHashes = set()
        Semaphore = asyncio.Semaphore(HashLookupLimit)
        
        async def ProcessCreator(Directory: str, CreatorName: str) -> None:
            try:
                async with Semaphore:
                    self.Log.debug(f'Looking Up Hashes For {CreatorName} From {Directory}...')
                    try:
                        Files = await asyncio.to_thread(
                            rclone.ls, 
                            f'{rclone.get_remotes()[-1]}{Directory}/{CreatorName}'
                        )
                        for File in [File['Name'] for File in Files]:
                            Hash = Path(File).stem
                            if len(Hash) >= 30:
                                ProcessedHashes.add(Hash[:30])
                    except RcloneException as Error:
                        self.ErrorLogger(Error)
                        self.Log.warning(f'Failed To Lookup Hashes For {CreatorName}')
                    
            except Exception as Error:
                self.ErrorLogger(Error)

        CreatorTasks = []
        for Platform in self.Data:
            for Directory in self.Data[Platform]['Directory'].values():
                try:
                    Creators = await asyncio.to_thread(
                        rclone.ls,
                        f'{rclone.get_remotes()[-1]}{Directory}'
                    )
                    for Creator in Creators:
                        CreatorTasks.append((Directory, Creator['Name']))
                except RcloneException:
                    self.Log.warning(f'Failed To Lookup Hashes For {Directory}')
        
        if CreatorTasks:
            self.Log.info(f'Looking Up Hashes From {len(CreatorTasks)} Creators With {HashLookupLimit} Parallel Workers...')
            
            Tasks = []
            for Directory, CreatorName in CreatorTasks:
                Tasks.append(asyncio.create_task(ProcessCreator(Directory, CreatorName)))
                if len(Tasks) >= HashLookupLimit * 4:
                    await asyncio.gather(*Tasks)
                    Tasks = []
            
            if Tasks:
                await asyncio.gather(*Tasks)
                
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
                    cookies={'session': self.Data[Platform]['Session']}
                ) as Response:
                    Response.raise_for_status()
                    Creators = orjson.loads(await Response.text())
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
        self.Log.debug(f'Fetched {Counter} Favorites')

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
                            Posts = orjson.loads(await Response.text())
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
    def __init__(self, Session: aiohttp.ClientSession, Log: logging.Logger, ErrorLogger: logging.Logger, Fetcher: Fetcher, SemaphoreLimit: int = 16) -> None:
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
        try:
            TotalSize = 0
            
            async with self.Session.get(Url) as Response:
                Response.raise_for_status()
                
                async with aiofiles.open(OutPath, 'wb') as f:
                    async for chunk in Response.content.iter_chunked(int(ChunkSize)):
                        if self.Stopped:
                            return 0
                        await f.write(chunk)
                        TotalSize += len(chunk)
            return TotalSize
        except Exception as Error:
            self.ErrorLogger(Error)

    async def Download(self, File: FileData) -> int:
        if self.Stopped:
            return 0

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
                    return 0

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
                        return 0
                    self.Hashes.add(str(File.Hash))
                    ElapsedTime = asyncio.get_event_loop().time() - StartTime
                    SpacePercentage = await self.CalculateSpacePercentage()
                    QueueStatus = f'[{self.Fetcher.DownloadQueue.qsize()}/{QueueLimit}]'
                    self.Log.info(
                        f'{QueueStatus} ({SpacePercentage}) Downloaded {File.Hash[:30]}... '
                        f'({await Humanize(FileSize)} in {ElapsedTime:.1f}s)'
                    ) if not self.Stopped else None
                    return FileSize
                return 0
            except Exception as Error:
                if any(isinstance(Error, ExceptionType) for ExceptionType in AiohttpExceptions + [BlockingIOError, RuntimeError, RetryError]):
                    pass
                else:
                    if not self.Stopped:
                        self.ErrorLogger(Error)
                        SpacePercentage = await self.CalculateSpacePercentage()
                        QueueStatus = f'[{self.Fetcher.DownloadQueue.qsize()}/{QueueLimit}]'
                        self.Log.warning(f'{QueueStatus} ({SpacePercentage}) Failed To Download {File.Hash[:30]}... ')
                return 0

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
        Log.info(f'Low Disk Space Threshold: {await Humanize(LowDiskSpaceThreshold)}')
        DownloadQueue = Queue(maxsize=QueueLimit)
        
        # Smart configuration with defaults
        FileSizeHistory = deque(maxlen=100)
        
        # Default semaphore limits - will be adjusted dynamically
        MinSemaphore = 8  # Default min threads
        MaxSemaphore = 24  # Default max threads
        DefaultSemaphore = 16  # Starting value
        CurrentSemaphoreLimit = DefaultSemaphore
        
        # Default creator fetch limits - will adjust based on queue
        MinCreatorFetches = 1
        MaxCreatorFetches = 5
        
        async def AdjustSemaphoreLimit():
            nonlocal CurrentSemaphoreLimit, MinSemaphore, MaxSemaphore
            while True:
                await asyncio.sleep(30)  # Adjust every 30 seconds
                
                # Smart default if we don't have enough data yet
                if len(FileSizeHistory) < 10:
                    # Keep current setting until we have enough data
                    continue
                    
                MedianSize = statistics.median(FileSizeHistory)
                
                # Auto-adjust min/max semaphore limits based on file size patterns
                if all(size < 1e6 for size in list(FileSizeHistory)[-10:]):
                    # If recent files are all small, increase max parallelism
                    MaxSemaphore = min(32, MaxSemaphore + 1)
                elif all(Size > 10e6 for Size in list(FileSizeHistory)[-10:]):
                    # If recent files are all large, decrease min parallelism
                    MinSemaphore = max(4, MinSemaphore - 1)
                
                # Calculate new limit based on file sizes
                if MedianSize < 1e6:  # < 1MB
                    NewLimit = MaxSemaphore
                elif MedianSize > 20e6:  # > 20MB
                    NewLimit = MinSemaphore
                else:
                    # Linear scale between min and max
                    Factor = 1 - ((MedianSize - 1e6) / (19e6))
                    RangeSize = MaxSemaphore - MinSemaphore
                    NewLimit = int(MinSemaphore + (Factor * RangeSize))
                    
                # Apply change if significant
                if abs(NewLimit - CurrentSemaphoreLimit) > 1:
                    Log.info(f'Adjusting Download Threads From {CurrentSemaphoreLimit} To {NewLimit} Based On Median File Size')
                    CurrentSemaphoreLimit = NewLimit
                    # Update the semaphore object
                    Download.Semaphore = asyncio.Semaphore(CurrentSemaphoreLimit)

        async def ProcessDownloads(Download: Downloader):
            while True:
                File = await DownloadQueue.get()
                try:
                    FileSize = await Download.Download(File)
                    if FileSize:
                        FileSizeHistory.append(FileSize)
                except Exception as Error:
                    ErrorLogger(Error)
                finally:
                    DownloadQueue.task_done()
        
        # Configure HTTP/2 Connector with default settings
        TCPConnector = aiohttp.TCPConnector(
            limit=DefaultSemaphore*8, 
            limit_per_host=DefaultSemaphore*4,
            ssl=False, 
            enable_cleanup_closed=True
        )
        
        Timeout = aiohttp.ClientTimeout(total=TimeoutConfig, connect=30.0, sock_connect=30.0, sock_read=TimeoutConfig)
        
        async with aiohttp.ClientSession(connector=TCPConnector, timeout=Timeout, 
                                        trust_env=True, raise_for_status=False) as Session:
            Fetch = Fetcher(Session, Log, ErrorLogger, DownloadQueue)
            Download = Downloader(Session, Log, ErrorLogger, Fetch, DefaultSemaphore)
            
            # Create dynamic semaphore for downloader
            Download.Semaphore = asyncio.Semaphore(CurrentSemaphoreLimit)
            
            # Start the downloader tasks - create more than needed for flexibility
            DownloadTasks = [
                asyncio.create_task(ProcessDownloads(Download))
                for _ in range(MaxSemaphore * 2)  # Create more tasks than semaphore allows
            ]
            
            # Start the semaphore adjustment task
            SemaphoreAdjustTask = asyncio.create_task(AdjustSemaphoreLimit())
            
            try:
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
                
                # Background Fetch Function
                async def BackgroundFetch():
                    CreatorQueue = asyncio.Queue()
                    for Creator in AllCreators:
                        await CreatorQueue.put(Creator)
                    
                    async def FetchCreatorPosts():
                        nonlocal MinCreatorFetches, MaxCreatorFetches
                        while not CreatorQueue.empty():
                            # Dynamically adjust parallel creator fetches based on queue size
                            QueuePercentage = DownloadQueue.qsize() / DownloadQueue.maxsize
                            if QueuePercentage > QueueThresholds[1]:
                                await asyncio.sleep(5)  # Queue high, wait a bit
                                continue
                                
                            # Calculate how many creators to fetch in parallel based on queue size
                            # Lower queue = more parallel fetches
                            AvailableFraction = 1 - (QueuePercentage / QueueThresholds[1])
                            ParallelFetches = max(
                                MinCreatorFetches,
                                min(MaxCreatorFetches, 
                                    int(AvailableFraction * MaxCreatorFetches))
                            )
                            
                            # Adjust the number of active fetcher tasks based on ParallelFetches
                            # (This is the missing part where ParallelFetches is used)
                            if len([t for t in FetcherTasks if not t.done()]) > ParallelFetches:
                                # Too many active tasks, wait a bit
                                await asyncio.sleep(1)
                                continue
                                
                            Creator = await CreatorQueue.get()
                            try:
                                await Fetch.Posts(Creator)
                            except Exception as Error:
                                ErrorLogger(Error)
                            finally:
                                CreatorQueue.task_done()
                    
                    # Create background fetcher tasks
                    FetcherTasks = [
                        asyncio.create_task(FetchCreatorPosts())
                        for _ in range(MaxCreatorFetches)
                    ]
                    
                    try:
                        await CreatorQueue.join()
                    finally:
                        for task in FetcherTasks:
                            task.cancel()
                        await asyncio.gather(*FetcherTasks, return_exceptions=True)
                
                # Start background fetching in parallel with downloads
                backgroundTask = asyncio.create_task(BackgroundFetch())
                
                # Wait for all tasks to complete
                await asyncio.gather(backgroundTask, DownloadQueue.join())
                
                Fetch.Log.info(f'Fetched {Fetch.TotalFiles} Files')
                Download.TotalFiles = Fetch.TotalFiles
                
            finally:
                SemaphoreAdjustTask.cancel()
                for Task in DownloadTasks:
                    Task.cancel()
                await asyncio.gather(*DownloadTasks, SemaphoreAdjustTask, return_exceptions=True)

            FileCount = sum(1 for _ in Path(FinalDir).rglob('*') if _.is_file())
            OptimalTransfers = await CalculateTransfers(FileCount)

            async with aiofiles.open('Data/Transfers.txt', 'w') as F:
                await F.write(str(OptimalTransfers))
            Log.info(f'Optimal Rclone Transfers: {OptimalTransfers} (Based On {FileCount} Files)')

    try:
        uvloop.run(Main()) if system() == 'Windows' else asyncio.run(Main())
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