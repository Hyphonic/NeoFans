# Main Imports
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError, retry_if_exception_type, wait_random
from tenacity.before_sleep import before_sleep_log
from dataclasses import dataclass
from typing import Union, Tuple
from json import JSONEncoder
from asyncio import Queue
from pathlib import Path
import aiofiles.os
import aiofiles
import asyncio
import aiohttp
import shutil
import psutil
import json
import sys
import os
import gc

# Logging
from rich.console import Console as RichConsole
from rich.traceback import install as Install
from rich.highlighter import RegexHighlighter
from rich.logging import RichHandler
from rich.theme import Theme
import logging

# Config
LowDiskSpaceThreshold = 10e+9
SemaphoreLimit = 8
QueueThresholds = [0.5, 0.8]
PageOffset = 50
StartingPage = 0

TimeoutConfig = aiohttp.ClientTimeout(
    total=300,
    connect=30,
    sock_read=60
)

# Logging Configuration
class DownloadHighlighter(RegexHighlighter):
    base_highlights = [
        r'(?P<yellow>#\d+)',  # Download counter
        r'(?P<cyan>[\d.]+ [KMGT]?B)',  # File sizes and disk space
        r'(?P<magenta>\d+/\d+)',  # Queue status
        r'(?P<cyan>[a-f0-9]{30})',  # File hashes
        r'(?P<green>Downloaded|Skipping)',  # Status words
        r'(?P<yellow>[\d.]+s)',  # Time values
        r'\[(?P<red>error|Error)\]',  # Error messages
        r'\[(?P<yellow>warning|Warning)\]',  # Warning messages
        r'\[(?P<green>info|Info)\]'  # Info messages
    ]

Console = RichConsole(
    theme=Theme({
        'log.time': 'bright_black',
        'logging.level.info': 'green',
        'logging.level.warning': 'yellow',
        'logging.level.error': 'red'
    }),
    force_terminal=True,
    width=120,
    log_path=False,
    highlighter=DownloadHighlighter()
)

# Single handler configuration
ConsoleHandler = RichHandler(
    markup=True,
    rich_tracebacks=True,
    show_time=True,
    console=Console,
    show_path=False,
    omit_repeated_times=True
)

# Set formatter once
ConsoleHandler.setFormatter(
    logging.Formatter('%(message)s', datefmt='[%H:%M:%S]')
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    handlers=[ConsoleHandler],
    force=True
)

# Configure Rich Logger
Log = logging.getLogger('rich')
Log.handlers.clear()
Log.addHandler(ConsoleHandler)
Log.propagate = False

def ErrorLogger(Error: Exception) -> None: 
    Console.print_exception(
        max_frames=1, 
        show_locals=True, 
        width=Console.width or 120
    )

Install(show_locals=True)

RetryConfig = dict(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2),
    before_sleep=before_sleep_log(Log, logging.WARNING),
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

Log.info(f'{QueueThresholds[0] * 100}% <-- Queue --> {QueueThresholds[1] * 100}%')

# Fetcher Class
class Fetcher:
    def __init__(self, Session: aiohttp.ClientSession, Log: logging.Logger, ErrorLogger: logging.Logger, DownloadQueue: Queue) -> None:
        self.Log = Log
        self.ErrorLogger = ErrorLogger
        self.Session = Session
        self.TotalFiles = 0
        self.DownloadQueue = DownloadQueue
        self.Stopped = False
        try:
            with open('Data/Hashes.json', 'r') as Hashes:
                self.Hashes = set(json.load(Hashes))
            self.Log.info(f'Loaded {len(self.Hashes)} Hashes')
        except FileNotFoundError:
            self.Log.warning('No Hashes Found!')
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
                    'Posts':    {
                            'patreon': [],
                            'subscribestar': [],
                            'gumroad': [],
                            'fanbox': []
                    }
                }
            }

    async def Favorites(self) -> None:
        self.Log.info('Fetching Favorites...')
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
        #self.Log.info(f'Fetching Posts From {Creator.Name}... ({Creator.ID})/{Creator.Service}')
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
                    self.Log.warning(f'Pausing Fetcher For {Creator.Name} - Queue At {self.DownloadQueue.qsize()}/{self.DownloadQueue.maxsize}')
                    while self.DownloadQueue.qsize() > (QueueThreshold * QueueThresholds[0]):
                        await asyncio.sleep(1)
                    self.Log.warning(f'Resuming Fetcher For {Creator.Name} - Queue At {self.DownloadQueue.qsize()}/{self.DownloadQueue.maxsize}')

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
                                    
                                    if str(FilePath.stem) not in self.Hashes:
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
                                            self.Log.warning(f'Download Queue Full ({self.DownloadQueue.qsize()}/{self.DownloadQueue.maxsize})')
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
                                        
                                        if str(FilePath.stem) not in self.Hashes:
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
        self.CompletedDownloads = 0
        self.TotalFiles = 0
        self.Stopped = False
        self.Fetcher = Fetcher
        try:
            with open('Data/Hashes.json', 'r') as Hashes:
                self.Hashes = set(json.load(Hashes))
        except FileNotFoundError:
            self.Hashes = set()
    
    @retry(**RetryConfig)
    async def FetchFile(self, Url: str) -> tuple[bytes, int]:
        async with self.Session.get(Url) as Response:
            if Response.status != 200:
                raise aiohttp.ClientError(f'Status: {Response.status}')
            Content = await Response.read()
            return Content, len(Content)
    
    async def Download(self, File: FileData) -> None:
        if self.Stopped:
            return
        
        if self.CompletedDownloads % 10 == 0:
            self.Log.info(f'{await Humanize(psutil.virtual_memory().available)} Available Memory')
            gc.collect()
        
        async with self.Semaphore:
            try:
                if str(File.Hash) in self.Hashes:
                    self.CompletedDownloads += 1
                    self.Log.warning(
                        f'#{self.CompletedDownloads} ({await Humanize(shutil.disk_usage(".").free)}) '
                        f'[{self.Fetcher.DownloadQueue.qsize()}/{self.Fetcher.DownloadQueue.maxsize}] [green]Skipping[/] '
                        f'{File.Hash[:30]}... '
                    ) if not self.Stopped else None
                    return

                try:
                    if shutil.disk_usage('.').free < LowDiskSpaceThreshold:
                        self.Log.warning('Low Disk Space!') if not self.Stopped else None
                        self.Stopped = True
                        self.Fetcher.Stopped = True
                        raise LowDiskSpace(f'Available Disk Space Below {await Humanize(shutil.disk_usage(".").free)}')
                
                    StartTime = asyncio.get_event_loop().time()
                    OutPath = Path('Data/Files') / File.Path.relative_to('Data')
                    await aiofiles.os.makedirs(OutPath, exist_ok=True)
                    
                    Content, FileSize = await self.FetchFile(File.Url)
                    async with aiofiles.open(OutPath / f'{File.Hash[:30]}{File.Extension}', 'wb') as F:
                        await F.write(Content)
                        self.Hashes.add(str(File.Hash))
                        self.CompletedDownloads += 1
                        ElapsedTime = asyncio.get_event_loop().time() - StartTime
                        self.Log.info(
                            f'#{self.CompletedDownloads} ({await Humanize(shutil.disk_usage(".").free)}) '
                            f'[{self.Fetcher.DownloadQueue.qsize()}/{self.Fetcher.DownloadQueue.maxsize}] [green]Downloaded[/] '
                            f'{File.Hash[:30]}... '
                            f'({await Humanize(FileSize)} in {ElapsedTime:.1f}s)'
                        ) if not self.Stopped else None
                        async with aiofiles.open('Data/Hashes.json', 'w') as f:
                            await f.write(json.dumps(list(self.Hashes)))
                except LowDiskSpace as Error:
                    raise Error
                except RetryError as Error:
                    if not self.Stopped:
                        self.ErrorLogger(Error)
                        self.Log.warning(
                            f'[#{self.CompletedDownloads}] Retry limit exceeded for {File.Hash[:30]}...'
                        )
                except Exception as Error:
                    if not self.Stopped:
                        self.ErrorLogger(Error)
                        self.Log.warning(
                            f'#{self.CompletedDownloads} ({await Humanize(shutil.disk_usage(".").free)}) '
                            f'[{self.Fetcher.DownloadQueue.qsize()}/{self.Fetcher.DownloadQueue.maxsize}] Failed To Download '
                            f'{File.Hash[:30]}... '
                        )
            except LowDiskSpace as Error:
                raise Error

async def Humanize(Bytes: int) -> str:
    for Unit in ['B', 'KB', 'MB', 'GB', 'TB']: 
        if Bytes < 1024.0:
            break
        Bytes /= 1024.0
    return f'{Bytes:.2f} [green]{Unit}[/]'

# JSON Encoder
class Encoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Path):
            return str(obj)
        if hasattr(obj, '__dataclass_fields__'):
            return {k: str(getattr(obj, k)) if isinstance(getattr(obj, k), Path) 
                   else getattr(obj, k) for k in obj.__dataclass_fields__}
        return super().default(obj)

if __name__ == '__main__':
    async def Main() -> None:
        DownloadQueue = Queue(maxsize=100)

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
            
            await Fetch.Favorites()
            
            for Platform in Fetch.Data:
                for Service in Fetch.Data[Platform]['Creators']:
                    for Creator in Fetch.Data[Platform]['Creators'][Service]:
                        await Fetch.Posts(Creator)
            
            Fetch.Log.info(f'Fetched [bold cyan]{Fetch.TotalFiles}[/] Files')
            Download.TotalFiles = Fetch.TotalFiles
            
            await DownloadQueue.join()
            for Task in DownloadTasks:
                Task.cancel()
            await asyncio.gather(*DownloadTasks, return_exceptions=True)
            
            await aiofiles.os.makedirs('Data', exist_ok=True)
            async with aiofiles.open('Data/Data.json', 'w', encoding='utf-8') as File:
                await File.write(json.dumps(
                    Fetch.Data,
                    indent=4,
                    ensure_ascii=False,
                    cls=Encoder
                ))

    try:
        asyncio.run(Main())
    except KeyboardInterrupt:
        Log.info('Exiting...')
        sys.exit(0)
    except LowDiskSpace as Error:
        Log.warning(Error)
        sys.exit(0)
    except Exception as Error:
        if not isinstance(Error, asyncio.CancelledError):
            ErrorLogger(Error)
        sys.exit(1)