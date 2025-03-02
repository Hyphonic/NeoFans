from rich.console import Console as RichConsole
from rich.traceback import install as Install
from rich.highlighter import RegexHighlighter
from rich.logging import RichHandler
from rich.theme import Theme
from rich.style import Style
import logging
import re

QueueLimit = 100

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
    Console.print_exception(max_frames=1, width=Console.width or 120)

def TestQueue():
    Log.info('Testing queue status coloring:')
    for q in range(0, QueueLimit+1, max(1, QueueLimit//10)):
        Log.info(f'Queue status: [{q}/{QueueLimit}]')

def TestPercent():
    Log.info('\nTesting percentage coloring:')
    for p in range(0, 101, 10):
        Log.info(f'Progress: {p}%')

Console, Log = InitLogging()
Install()

if __name__ == '__main__':
    TestQueue()
    TestPercent()