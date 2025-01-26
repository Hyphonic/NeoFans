from concurrent.futures import ThreadPoolExecutor
from rich.console import Console
from rich.table import Table
from typing import Dict
from rich import box
import urllib.parse
import threading
import requests
import datetime
import aiofiles
import aiohttp
import asyncio
import random
import queue
import time
import html
import json
import os
import re

from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
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

Config = {
    'rule34': {
        'creator_limit': 1000,
        'ids': [
            'liepraag', 'jackerman', 'rule34lab', 'tdontran', '0zmann', 'user:FloorPudding',
            'bewyx', 'rinny', 'yeero', 'guiltyk', 'dcd', 'billie_eilish+3d+video', 'radroachhd',
            'flarewizz', 'biggreen', 'nyl2', '2c3d', 'sleepykeeper', 'scrag_boy', 'skidz44',
            'milk_itai', 'pein', 'floppyhuman', 'smitty34', 'skuddbutt', 'nixmare_(artist)',
            'forsaken_(artist)', 'sfmslayer', 'sus_biff', 'lusty_eyes', 'shizzyzzzzzz', 'darksub',
            'babymoonart', 'adriandustred', 'animopron', 'omitome', 'nagoonimation', 'sasakywiz',
            'gasprart', 'wyerframez', 'selfdrillingsms', 'deepstroke', 'noname55', 'vaerhonfx',
            'mept44', 'rougenine', 'bonkge', 'thecount', 'slayed.coom', 'thebartender', 'mossited',
            'lucislab', 'nyx34x', 'shortstackmagx2', 'kinora3d', 'boobly', 'bloggerman', 'user:Wufflezz',
            'idemi-iam', 'sulbas3d', 'vgerotica', 'meis3d', 'hydrafxx', 'bamh3d', 'sessho3d',
            'crisisbeat', 'axenanim', 'vchansfm', 'red404', 'birdx3d', 'cruiser_d.va', 'misterorzo',
            'adieonart', 'rosemary_winters', 'k/da_all_out_seraphine', 'doomfella', 'curvylover3d',
            'redhoney.ai', 'puffyart', 'licaidev', 'smesh_(artist)', 'psyopsai', 'somenerdygentleman',
            'themadcommander', 'x_redeyes', 'xazter3d', 'jollyferret', 'cheesyx3d', 'slop_art',
            'anaru', 'tastysuprise', 'srasoni01', 'phosaggro', 'babymoon', 'xaz3d', 'akkonsfw',
            '3eeq', 'almightypatty', 'jpbtos', 'frolich', 'kassioppiava', 'pantsushi',
            'oatmealpecheneg', 'mslewd', 'nuttycravat', 'sandflyactual', 'raz0r33', 'user:Sherlock910',
            'kittenvox', 'fennochik', 'monarchnsfw', 'chloeangelva', 'fpsblyck', 'fazysloww',
            'rexlapix', 'initial_a', 'seejaydj', 'sinilyra', 'conseitnsfw', 'saberwolf8',
            'scarecraw_', 'justausernamesfm', 'rekin3d', 'woudlwonker819', 'plague_of_humanity_(artist)',
            'meltoriku', 'chittercg', 'bamboozlicious', 'ruru3dx', 'misthiosarc', 'zonkyster',
            'cpt-flapjack', 'gcraw', 'delalicious3', 'hold3dx', 'yellowbea', 'sexx3d-art',
            'deezoy', 'regina3d', 'mantis_x', 'bouquetman', 'dash23', 'polsy', 'z1g3d',
            'blackbirdnsfw', 'tyviania', 'blenderknight', 'nuttytouch', 'khaidow_a_roy',
            'aphy3d', 'amusteven', 'teenx', 'captain_ameba', 'darkbahamuth', 'twitchyanimation',
            'maiden-masher', 'pleasethisworks', 'ulfsark3d', 'zxxxarts', 'super_elon',
            'skxx_elliot', 'nastytentaclee', 'cosmic_trance', 'x3milky', 'derpixon', 'glengrantsquid',
            'generalbutch', 'midnight_datura', 'peterraynor', 'volkor', 'salsen3d', 'vensaku',
            'kittyyevil', 'itslaiknsfw', 'cakiibb', 'cerbskies', 'abarus', 'dme_nsfw',
            'snuddy', 'monna_haddid', 'reynydays', 'stevencarson', 'pillowfun', 'greengiant3d',
            'eddysfm', 'fugtrup', 'neroxliv', 'amazonium', 'dominothecat', 'ruidx', 'sfrinzy',
            'piroguh', 'evilbaka', 'rasmus-the-owl', 'mikadawn', 'apone3d', 'hood_nsfw', 'howlsfm',
            'privateotgx', 'puzz3d', 'sampples', 'lazysoba', 'lazyprocrastinator', 'guigz1',
            'mehlabs', 'xavier3d', 'vivacious3d', 'hornywitches3d', 'jos_bobot', 'rexart',
            'wet_cheetos', 'elizabeth_comstock+3d+pinup', 'shirosfm', 'amiris4', 'elferan',
            'lordofpeaches', 'dtee3d', 'the_qbd', 'coresvoid', 'serahnsfwart', 'gifdoozer',
            'sosiskaba6y', '4th_rate', 'nordfantasy', 'giocamolly', 'fgnilin', 'arhoangel',
            'theuchihakid', 'cheesternsfw', 'project_vega', 'hedonicphenomenon', 'quadraticsfm',
            'alexavlewd', 'raxinsfw', 'smutnysize', 'redauge', 'salamandraninja', 'scenezyn',
            'soulartmatter', 'laik3d', 'void3d', 'cixf', 'takerskiy', 'moby_(artist)', 'vtmeen',
            'comandorekin', 'vool', 'wintterarts', 'robsnsfw', 'that_maskey', 'blushymostly', 'insetick',
            'm-rs', 'hooves-art', 'rwt4184', 'theduudeman', 'lickiz', 'lucosmico', 'keister3d',
            'otacon212', 'francis_brown', 'noahgraphicz', 'thenakedmonster', 'kodaknsfw', 'vonsvaigen',
            'waywardblue', 'suifuta', 'zen_art', 'heebiejeebus', 'asaron', 'empnsfw', 'manufatura',
            'eggsnsfw', 'blackedr34', 'pervertmuffinmajima', 'femshoptop+breasts', 'ky0rii', 'hyartik',
            'loveslove', 'sfrogue', 'reinbou', 'clubzenny', 'takita_tamalero', 'yazanios', 'kaylzara',
            'reinamation3d', 'kachigachi', 'merkaan', 'beowulf1117', 'valenok', 'sixser', 'masterzenus',
            'milapone', 'project_beast', 'enigmaj', 'durabo', 'chloe_(detroit:_become_human)',
            'minkoanimation', 'pumpalooza', 'moxx3d', 'rympha3d', 'manbaburakku', 'raix_xx', 'sevenbees',
            'chrisisboi_', '26regionsfm', 'random_tide', 'yasminnora', 'athazel', 'kookrak', 'skelly3d',
            'donan', 'kreseks', 'rude_frog', 'theoneavobeall7(artist)', 'hasfeldt', 'dubzzzy', 'ws3d',
            'sotb1337', 'loams3d', 'thevercetti', 'lazper', 'toonleak', 'leodamodeler', 'studio_null',
            'futaprisoner', 'shadman', 'pornlandlord', 'ghoulishxxx', 'breadblack', 'shark2j', 'breedbowl',
            'ekke', 'nyxenartz', 'cybrokrimson', 'nottanj', 'entitledgoose', 'futarush', 'nullus_02',
            'queenelsamodern', 'madrugasfm', 'virtualxtacy', 'mafuyur34', '2hour2', 'oscillator', 'user:AnimeGirl39',
            'nsf3d', 'kishi', 'checkpik', 'skyarsenic', 'novah3d', '27bits', 'grand_cupido', 'unveilingavidity',
            'emberstock', 'clixmansfw', 'dasupanoob', 'bayernsfm', 'jizzmasterzero', 'rayna_k1', 'snafusevsix',
            'artsbyronin', 'lawyernsfw', 'aeteranix', 'feversfm', 'ceeeeekc', 'antidotetrl', 'pearforceone',
            'spookieshade', 'erotichris', 'rosemary_winters+3d+pinup', 'son_umbasa', 'realm3do', 'blindparty',
            'dzooworks', 'citrus2077', 'the_firebrand', 'wildynsfw', 'zentaeron', 'sekaithereturn', 
            'mersure', 'foulveins', 'hotsteak', 'vekkte', 'sanmie3d', 'rescraft', 'pentraxnsfw', 'user:Minimaxi01',
            'octodog3d', 'naifu', 'forged3dx', 'echiee', 'arti202', '3dwick', 'vreal_18k', 'wawmes',
            'wtfsths', 'chikipiko_(artist)', 'fireboxstudio', 'empusaau', 'pluto3d', 'siliconaya', 'haziest_mirage',
            'slenderrender', 'vicer34', 'shibashake', 'resteel', 'anilvl', 'chronoai', 'csr55', 'tomixp', 'hstudios',
            'aliusnext', 'ahegao_ai', 'jaajaai', 'luxruleart', 'karfex', 'jumboxx', '3eedeebee', 'nekonyan',
            'shellraiser', 'idel_art', 'ryuuziken01', 'coffeeforsnails', 'bobcat_nsfw', 'm59ai', 'voidpilgrim',
            'eraofwave', 'aippealing', 'lustynari', 'r34arts', 'docni', 'supermanson', 'poncedart', 'quietdreams3d',
            'xlily666x', 'suoiresnu', 'lm19', 'realistic+ai_generated', 'miwo3x', 'haadxee', 'secret_room12', 'user:fnafsmash99',
            'nexynsfw', 'bhruu', 'dreamyai', 'braskymin', "belethor's_smut", 'bobbysnaxey', 'milkedwaifus', 'shennru',
            'yashugai', 'fizzz', 'kenjjoo', 'shadowboxer', 'bigrx', 'freakfestai', 'torl', 'radnsad', 'sisko11132',
            'lavah', 'nemesis_3d', 'dopyteskat', 'dinixdream', 'rousherte', 'psicoero', 'naughtygirlsai', 'user:animegirl39+3d',
            'edosynf', 'electroworld', 'creamybiscuit', 'veve', 'zfapai', 'igor0914', 'the_unhindered_fool', 'silver2299',
            'santopati', 'hyoombotai', 'altra_x', 'synthpixel', 'velzevulito', 'steely_bird', 'saytwwo', 'futagallery',
            'luvnari', 'jousneystudio', 'kit_s_une', 'blenderdemon', 'noirmusensfw', 'blueberg', 'monsterart', 'lampero',
            'chris_hopers', 'fluffydisk42', 'jamesbron', 'mikey-rg97', 'wrath555', 'selfmindsources', 'raxastake',
            'sourwayne', 'sensanari', 'perry_cracker', 'fremorg', 'thearti3d', 'dinoboy555' 'demonlorddante', 'liphisbus',
            'gameoversfm', 'addictedbud', 'erslait', 'sadtomato', 'nocure21o', 'thedirtden', 'osimai', 'velonix',
            'oneyedemperor', 'icarusillustrations', 'cga3d', 'hotcartoonai', 'eidedodidei', 'nayvenn', 'fundoshilover101',
            'lolicon_hunter', 'nsfwdestiny', 'poosan', 'tyrvax', 'zeta_saint', 'alpixy', 'ninfrock', 'sensualcreations', 
            'dezmall', 'vexonair', 'aibro', 'ceoofpaws', 'lucasai', 'kinkyra', 'progenarts', 'edalv', 'elygordanart', 
            'marshalperv', 'saveass_', 'omega_weirdo', 'jolol44', 'kexonik', 'serlord', 'icedev','xwaifuai', 'plaitonic',
            'creatronart', 'jorenran', 'mando_logica', 'thethiccart', 'yui_main_dbd', 'peachsoda3d', 'user:NSFWERSTON',
            'newgenai', 'alluring-artwork', 'killjoyproject', 'trixie_tang', '3derotica', 'macklesternsfw', 'krauswnori',
            'borvar', 'xiiirockiiix', 'a1exwell', 'mrveryoliveoil', 'zoulihd', 'vexingvenery', 'quick_e', 'thecoomjurer',
            'blenderanon_', 'sunk118', 'trist_ava', 'zux_edits', 'lewdneeko', 'thefoxxx', 'kitedout', 'bloo3d',
            'weedson86', 'meat_master', 'kumbhker', 'thefastestgman', 'shibarademu', 'lexy_3d', 'zyx3dx', 'user:Myth1c',
            'currysfm', 'fatcat17', 'pixel3d', 'nokeb', 'kanna+nyotengu', 'rumbleperv', 'zis2nsfw', 'jvfemdom',
            'missally', 'emini_ruru', 'elklordart', 'onagi', 'mhpwrestling', 'bai3d', 'sentinel1_4', 'chadrat',
            'jojozz', 'raviana_brwl', 'klodow', 'toasted_microwave', 'nithes', 'creationmach1ne', 'hobossy', 'bigzbig',
            'hagiwara_studio', 'redhenxx', 'grvty3d', 'ashorix', 'audrix', 'perpetualabyss', 'supurattabrain',
            'tm04', 'rataddict', 'binibon123', 'strauzek', 'uzachan', 'acacklinghyena', 'rebit', '3difill',
            'frankie_foster+pinup', 'user:DegensAI', 'user:Evelynn16', 'ninesix', 'partnitex', 'd1vit', 'verybadboye',
            'renmax3d', 'sirkyer', 'lunex3d', 'zyneos', 'zaddycat', 'operculum', 'empathetic-one', 'kiriyawn',
            'pensubz', 'emilygrace3d', 'garlock', 'grart', 'alicecry', 'dreamcuc', 'nashandraffxiv', 'xieangel',
            'friendship_is_magic+3d+ai_generated', 'kinkykatt3d', 'godwin', 'bottopbot2', 'user:vowod40853ll'
            ],
        'names': [],
        'directory_names': []
    },
    'onlyfans': {
        'creator_limit': 1000,
        'ids': [],
        'names': [],
        'directory_names': []
    },
    'fansly': {
        'creator_limit': 1000,
        'ids': [],
        'names': [],
        'directory_names': []
    },
    'patreon': {
        'creator_limit': 1000,
        'ids': [],
        'names': [],
        'directory_names': []
    },
    'subscribestar': {
        'creator_limit': 1000,
        'ids': [],
        'names': [],
        'directory_names': []
    },
    'e621': {
        'creator_limit': 1000,
        'ids': [
            'floppyhuman', 'xlkev', 'babymoon', 'manwiththemole', 'whisperfoot', 'jinsang_(artist)',
            'lewdchord', 'softkathrin', 'loveslove', 'steamyart', 'nocure21o', 'anonymousfm',
            'screwingwithsfm', 'chadrat', 'erostud', 'frostbound', 'burrymare', 'hooves-art',
            'pochemu', 'cumkeys', 'fluxcore', 'dcd', 'bamb00', 'rubber_(artist)', 'reinbou',
            'antonsfms', 'frihskie', 'operculum', 'hornyforest', 'yoracrab', 'futuretist', 'magnetvox',
            'drboumboom32+-male_penetrating_male', 'cyberkaps', 'dividebyezer0', 'scafen_(artist)',
            'seraziel', 'keffotin', 'danilokumriolu', 'foxinuhhbox', 'mixedtoofer', 'the_man',
            'mawmain', 'chelodoy', 'ultrabondagefairy', 'twistedscarlett60', 'bonifasko',
            'marblesoda', 'unattendedmilk', 'ksenik', 'alex_artist', 'papayathebun', 'lunarii',
            'sataenart', 'panken', 'vesper_art', 'zerlix_fox', 'blackjr',
            'wildblur', 'cooliehigh', 'marrubi_(artist)', 'tsunoart', 'masik00masik', 'rodd.y',
            'eepytune', 'ruvark', 'catniplewds', 'hacatiko', 'cerf', 'fluffybrownfox', 'fiamourr',
            'blulesnsfw', 'nepentz', 'xaz3d', 'holymeh', 'ashraely', 'zypett'
        ],
        'names': [],
        'directory_names': []
    },
    'fanbox': {
        'creator_limit': 1000,
        'ids': [],
        'names': [],
        'directory_names': []
    },
    'gumroad': {
        'creator_limit': 1000,
        'ids': [],
        'names': [],
        'directory_names': []
    },
    'directory_names': {
        'rule34': '🎨 Rule34',
        'onlyfans': '🌀 OnlyFans',
        'fansly': '🔒 Fansly',
        'patreon': '🅿️ Patreon',
        'subscribestar': '⭐ SubscribeStar',
        'e621': '🐾 E621',
        'fanbox': '📦 Fanbox',
        'gumroad': '💗 Gumroad'
    },
    'platform_names': {
        'rule34': '[aquamarine3]Rule34[/aquamarine3]',
        'onlyfans': '[cornflower_blue]OnlyFans[/cornflower_blue]',
        'fansly': '[dodger_blue2]Fansly[/dodger_blue2]',
        'patreon': '[salmon1]Patreon[/salmon1]',
        'subscribestar': '[dark_cyan]SubscribeStar[/dark_cyan]',
        'e621': '[deep_sky_blue4]E621[/deep_sky_blue4]',
        'fanbox': '[sky_blue1]Fanbox[/sky_blue1]',
        'gumroad': '[hot_pink]Gumroad[/hot_pink]'
    },
    'threads': {
        'max_workers': 32
    },
    'enabled_platforms': {  # Override creator_limit to 0 if platform not in enabled_platforms
        'rule34': True,
        'onlyfans': True,
        'fansly': True,
        'patreon': True,
        'subscribestar': True,
        'e621': True,
        'fanbox': True,
        'gumroad': True
    },
    'global_limit': 500,
    'dry_run': False,
    'platform_limit_debug': 1,
    'cached_hashes': {}  # Initialize cached_hashes
}

class FavoriteFetcher:
    def __init__(self, Platform):
        try:
            # Load last fetch dates from json
            with open('last_fetches.json', 'r') as f:
                self.LastFetches = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.LastFetches = {}
            
        try:
            # Load cached hashes per creator
            with open('cached_hashes.json', 'r') as f:
                self.CachedHashes = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.CachedHashes = {}

        Url = f'https://{Platform}.su/api/v1/account/favorites?type=artist'
        Session = requests.Session()
        Session.headers.update({
            'accept': 'application/json',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (SMART-TV; Linux; Tizen 5.0) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/2.2 Chrome/63.0.3239.84 TV Safari/537.36'
        })

        Session.cookies.set(
            'session',
            os.getenv('COOMER_SESS') if Platform == 'coomer' else os.getenv('KEMONO_SESS'),
            domain=f'{Platform}.su',
            path='/'
        )

        Response = Session.get(Url)
        
        if Response.status_code == 200:
            Data = Response.json()

            for Service in ['onlyfans', 'fansly', 'patreon', 'subscribestar', 'fanbox', 'gumroad']:
                # Initialize service in Config if needed
                if 'ids' not in Config[Service]:
                    Config[Service]['ids'] = []
                if 'names' not in Config[Service]:
                    Config[Service]['names'] = []
                    
                # Initialize service in cache structures if needed
                if Service not in self.LastFetches:
                    self.LastFetches[Service] = {}
                if Service not in self.CachedHashes:
                    self.CachedHashes[Service] = {}

            for Item in Data:
                Service = Item.get('service')
                CreatorId = Item.get('id')
                CreatorName = Item.get('name')
                Config[Service]['ids'].append(CreatorId)
                Config[Service]['names'].append(CreatorName)

                
        Session.close()

############################################################
#                                                          #
#                     Download Manager                     #
#                                                          #
############################################################

class AsyncDownloader:
    def __init__(self, ChunkSize=1024*1024):
        self.ChunkSize = ChunkSize
        self.Session = None
        self.Semaphore = asyncio.Semaphore(32)  # Limit concurrent connections
        
    async def InitSession(self):
        self.Session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, connect=60),
            connector=aiohttp.TCPConnector(limit=32)
        )
        
    async def CloseSession(self):
        if self.Session:
            await self.Session.close()
            
    async def DownloadFile(self, FileData, ProgressCallback):
        FileHash, FileUrl, FilePath = FileData
        async with self.Semaphore:
            try:
                async with self.Session.get(FileUrl) as Response:
                    if Response.status == 200:
                        if not Config['dry_run']:
                            os.makedirs(os.path.dirname(FilePath), exist_ok=True)
                            Downloaded = 0
                            FileSize = int(Response.headers.get('content-length', 0))
                            
                            async with aiofiles.open(FilePath, 'wb') as f:
                                async for Chunk in Response.content.iter_chunked(self.ChunkSize):
                                    if Chunk:
                                        await f.write(Chunk)
                                        Downloaded += len(Chunk)
                                        ProgressCallback(Downloaded, FileSize, FileHash)
                        else:
                            # Simulate download
                            FileSize = int(Response.headers.get('content-length', 1024*1024))
                            Downloaded = 0
                            while Downloaded < FileSize:
                                await asyncio.sleep(0.05)
                                ThisChunk = min(self.ChunkSize, FileSize - Downloaded)
                                Downloaded += ThisChunk
                                ProgressCallback(Downloaded, FileSize, FileHash)
                                
                        return True
                return False
            except Exception:
                return False

class DownloadManager:
    def __init__(self, FileList, FetcherInstance):
        self.FileList = FileList
        self.Fetcher = FetcherInstance
        self.Stats = {
            'TotalBytes': 0,
            'FailedFiles': 0,
            'SuccessfulFiles': 0,
            'TotalFiles': len(FileList),
            'StartTime': time.time(),
            'EndTime': None,
            'AverageSpeed': 0,
            'CurrentFile': "Initializing..."
        }
        self.Stats.update({
            'FetchTime': 0,
            'DownloadTime': 0,
            'DirectoryTime': 0,
            'TotalDownloaded': 0,
            'AverageFileSize': 0,
            'PeakSpeed': 0,
            'SuccessRate': 0
        })
        self.StatsLock = threading.Lock()
        self.MaxWorkers = Config['threads']['max_workers']
        self.MinSpace = 5e+9
        self.StopDownloads = False
        self.DryRun = Config.get('dry_run', False)
        self.ProcessedFiles = 0
        self.CachedHashes = {}
        self.NewHashes = {}
        self.LoadCachedHashes()
        self.ProcessedHashes = set()  # Track processed hashes to prevent duplicates
        self.CompletedFiles = set()  # Track completed files globally
        self.FileQueue = queue.Queue()  # Queue for files to download
        for FileData in FileList:
            self.FileQueue.put(FileData)
        
    def LoadCachedHashes(self):
        """Load previously downloaded file hashes per platform and creator"""
        try:
            with open('cached_hashes.json', 'r') as f:
                self.CachedHashes = json.load(f)
            Config['cached_hashes'] = self.CachedHashes  # Update Config
            Logger.Debug(f"Loaded cached hashes for {len(self.CachedHashes)} platforms")
        except (FileNotFoundError, json.JSONDecodeError):
            Logger.Debug("No cached hashes found, starting fresh")
            self.CachedHashes = {}
            Config['cached_hashes'] = self.CachedHashes  # Initialize in Config
            
    def SaveNewHashes(self):
        """Save new hashes to cache file, organized by platform and creator"""
        if self.NewHashes:
            try:
                # Merge new hashes with existing ones
                for Platform in self.NewHashes:
                    if Platform not in self.CachedHashes:
                        self.CachedHashes[Platform] = {}
                    
                    for Creator in self.NewHashes[Platform]:
                        if Creator not in self.CachedHashes[Platform]:
                            self.CachedHashes[Platform][Creator] = []
                        
                        # Add new unique hashes
                        self.CachedHashes[Platform][Creator] = list(set(
                            self.CachedHashes[Platform][Creator] + 
                            self.NewHashes[Platform][Creator]
                        ))
                
                with open('cached_hashes.json', 'w') as f:
                    json.dump(self.CachedHashes, f, indent=4)
                
                TotalHashes = sum(
                    len(Hashes) 
                    for Platform in self.NewHashes 
                    for Creator, Hashes in self.NewHashes[Platform].items()
                )
                Logger.Debug(f"Saved {TotalHashes} new hashes to cache")
            except Exception as e:
                Logger.Error(f"Failed to save hashes: {e}")
        
    def PrintSummary(self):
        """Print formatted summary table of the download operation"""
        SummaryTable = Table(
            title="\n[bold cyan]Download Summary[/bold cyan]",
            show_header=True,
            header_style="bold magenta",
            border_style="blue",
            box=box.ROUNDED
        )
        
        # Add columns
        SummaryTable.add_column("Metric", style="cyan", no_wrap=True)
        SummaryTable.add_column("Value", style="green")
        
        # Calculate additional stats
        TotalTime = time.time() - self.Stats['StartTime']
        SuccessRate = (self.Stats['SuccessfulFiles'] / self.Stats['TotalFiles']) * 100 if self.Stats['TotalFiles'] > 0 else 0
        AverageSpeed = self.Stats['TotalDownloaded'] / TotalTime if TotalTime > 0 else 0
        AverageFileSize = self.Stats['TotalDownloaded'] / self.Stats['SuccessfulFiles'] if self.Stats['SuccessfulFiles'] > 0 else 0
        
        # Add rows
        SummaryTable.add_row("Operation Mode", "[yellow]Dry Run[/yellow]" if self.DryRun else "[green]Real Download[/green]")
        SummaryTable.add_row("Total Files", str(self.Stats['TotalFiles']))
        SummaryTable.add_row("Successfully Downloaded", f"{self.Stats['SuccessfulFiles']} ({SuccessRate:.1f}%)")
        SummaryTable.add_row("Failed Downloads", str(self.Stats['FailedFiles']))
        SummaryTable.add_row("Total Data Downloaded", self.HumanizeBytes(self.Stats['TotalDownloaded']))
        SummaryTable.add_row("Average File Size", self.HumanizeBytes(AverageFileSize))
        SummaryTable.add_row("Average Speed", f"{self.HumanizeBytes(AverageSpeed)}/s")
        SummaryTable.add_row("Peak Speed", f"{self.HumanizeBytes(self.Stats['PeakSpeed'])}/s")
        SummaryTable.add_row("Total Time", self.HumanizeTime(TotalTime))
        SummaryTable.add_row("Skipped (Cached)", str(self.Stats.get('SkippedFiles', 0)))
        
        Console().print(SummaryTable)
        
    def Download(self):
        try:
            StartTime = time.time()
            ProgressColumns = [
                TextColumn("[cyan]Downloading"),
                BarColumn(bar_width=40),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("•"),
                TextColumn("[blue]{task.fields[current_file]}"),
                TextColumn("•"), 
                TextColumn("{task.fields[file_size]}"),
                TextColumn("•"),
                MofNCompleteColumn(),
                TextColumn("•"),
                TextColumn("[yellow]{task.fields[speed]}/s"),
                TimeRemainingColumn(),
            ]

            with Progress(*ProgressColumns, console=Console(force_terminal=True), auto_refresh=False) as ProgressBar:
                MainTask = ProgressBar.add_task(
                    "Downloading",
                    total=self.FileQueue.qsize(),
                    current_file="Initializing...",
                    file_size="0 B",
                    speed="0 B/s"
                )

                def ProcessFile():
                    while not self.FileQueue.empty() and not self.StopDownloads:
                        try:
                            FileData, Platform, Creator = self.FileQueue.get_nowait()
                        except queue.Empty:
                            return

                        FileHash, FileUrl, FilePath = FileData

                        # Skip if already downloaded
                        if FileHash in self.CompletedFiles:
                            self.FileQueue.task_done()
                            continue

                        try:
                            # Get file size before downloading
                            Response = requests.head(FileUrl, allow_redirects=True)
                            if Response.status_code != 200:
                                self.FileQueue.task_done()
                                continue

                            FileSize = int(Response.headers.get('content-length', 0))
                            FileExtension = os.path.splitext(FileUrl)[1]

                            if self.DryRun:
                                time.sleep(0.1)
                                with self.StatsLock:
                                    self.ProcessedFiles += 1
                                    self.CompletedFiles.add(FileHash)
                                    ProgressBar.update(MainTask, advance=1)
                                    ProgressBar.refresh()
                                self.FileQueue.task_done()
                                continue

                            # Actual download
                            Response = requests.get(FileUrl, stream=True)
                            if Response.status_code == 200:
                                os.makedirs(os.path.dirname(FilePath), exist_ok=True)
                                Downloaded = 0
                                StartTime = time.time()

                                with open(FilePath, 'wb') as f:
                                    for Chunk in Response.iter_content(chunk_size=1024*1024):
                                        if Chunk:
                                            f.write(Chunk)
                                            Downloaded += len(Chunk)
                                            ElapsedTime = time.time() - StartTime
                                            Speed = Downloaded / ElapsedTime if ElapsedTime > 0 else 0

                                            ProgressBar.update(
                                                MainTask,
                                                current_file=f"{FileHash[:20]}..{FileExtension}",
                                                file_size=self.HumanizeBytes(FileSize),
                                                speed=self.HumanizeBytes(Speed)
                                            )
                                            ProgressBar.refresh()

                                with self.StatsLock:
                                    self.ProcessedFiles += 1
                                    self.CompletedFiles.add(FileHash)
                                    self.Stats['TotalDownloaded'] += Downloaded
                                    self.Stats['PeakSpeed'] = max(self.Stats['PeakSpeed'], Speed)
                                    ProgressBar.update(MainTask, advance=1)
                                    ProgressBar.refresh()

                        except Exception as e:
                            Logger.Error(f"Download error for {FileHash}: {str(e)}")
                        finally:
                            self.FileQueue.task_done()

                # Use ThreadPoolExecutor for concurrent downloads
                with ThreadPoolExecutor(max_workers=self.MaxWorkers) as Executor:
                    Futures = [
                        Executor.submit(ProcessFile)
                        for _ in range(min(self.MaxWorkers, self.FileQueue.qsize()))
                    ]
                    
                    # Wait for all tasks to complete
                    self.FileQueue.join()

            self.Stats['DownloadTime'] = time.time() - StartTime
            self.SaveNewHashes()
            self.PrintSummary()
            return True

        except Exception as e:
            Logger.Error(f"Error in download process: {e}")
            return False

    @staticmethod
    def HumanizeBytes(Bytes):
        for Unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if Bytes < 1024:
                return f"{Bytes:.2f} {Unit}"
            Bytes /= 1024

    @staticmethod
    def HumanizeTime(Seconds):
        return str(datetime.timedelta(seconds=Seconds))
    
    def GetFileSize(self, Url):
        # Method to get the size of the file from the URL
        Response = requests.head(Url)
        if 'Content-Length' in Response.headers:
            return int(Response.headers['Content-Length'])
        return 0

    # Remove the ExtractPathInfo method
    # def ExtractPathInfo(self, FilePath):
    #     # ...unused method...
    #     return None, None

############################################################
#                                                          #
#                         Fetcher                          #
#                                                          #
############################################################

class Fetcher:
    def __init__(self, Platform, Id, Name, DirectoryName, CachedHashes, CreatorLimit, GlobalLimit):
        self.Page = 0
        self.Session = requests.Session()

        self.Session.headers.update({
            'accept': 'application/json',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (SMART-TV; Linux; Tizen 5.0) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/2.2 Chrome/63.0.3239.84 TV Safari/537.36'
        })

        try:
            ProxyType = random.choice(['http', 'socks4', 'socks5'])
            self.Session.proxies.update({
                ProxyType: random.choice(open(f'proxies/{ProxyType}.txt').read().splitlines())
            })
        except Exception:
            pass

        #Logger.Debug(f'({ProxyType}) Using proxy {self.Session.proxies[ProxyType]}')

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

    def ExtractHash(self, Url):  # Extract hash from URL / <hash> .png
        if not Url:
            return None
        # Extract the filename from the URL and split on last dot
        Filename = Url.split('/')[-1]
        return Filename.rsplit('.', 1)[0]

    def FetchUrl(self, Url: str, Params: Dict = None) -> Dict:
        try:
            Response = self.Session.get(Url, params=Params)
            if Response.status_code == 200:
                return Response.json(), Response.status_code
            return None, Response.status_code
        except Exception:
            return None, None

    def ExtractLinks(self, Content) -> str:
        # Decode HTML entities using html library
        Content = html.unescape(Content)
        # Regex to find URLs
        UrlPattern = re.compile(r'https?://[^\s<]+')
        Urls = UrlPattern.findall(Content)
        Logger.Debug(f'Found {len(Urls)} URLs')
        for Url in Urls:
            Logger.Debug(f'∙ {Url}')
        return Urls

    def Scrape(self):

        ############################################################
        #                                                          #
        #                          Rule34                          #
        #                                                          #
        ############################################################

        if self.Platform == 'rule34':
            BaseParams = dict(urllib.parse.parse_qsl(self.Params))
            while self.GlobalLimit > 0 and self.CreatorLimit > 0:
                BaseParams["pid"] = self.Page
                ReEncodedParams = urllib.parse.urlencode(BaseParams, safe="+")
                Response, StatusCode = self.FetchUrl('https://api.rule34.xxx/index.php', ReEncodedParams)
                
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

                                if FileHash and FileHash in self.CachedHashes:
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
                Response, StatusCode = self.FetchUrl("https://e621.net/posts.json", ReEncodedParams)
                
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
                                if FileHash and FileHash in self.CachedHashes:
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
            Response, StatusCode = self.FetchUrl(f'https://{Hoster}.su/api/v1/{self.Platform}/user/{self.Id}')
            
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
                                
                                if FileHash and FileHash not in self.CachedHashes:
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
                                
                                if FileHash and FileHash not in self.CachedHashes:
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

        self.Session.close()
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

Screen = r'''

 __   __     ______     ______     ______   ______     __   __     ______    
/\ "-.\ \   /\  ___\   /\  __ \   /\  ___\ /\  __ \   /\ "-.\ \   /\  ___\   
\ \ \-.  \  \ \  __\   \ \ \/\ \  \ \  __\ \ \  __ \  \ \ \-.  \  \ \___  \  
 \ \_\\"\_\  \ \_____\  \ \_____\  \ \_\    \ \_\ \_\  \ \_\\"\_\  \/\_____\ 
  \/_/ \/_/   \/_____/   \/_____/   \/_/     \/_/\/_/   \/_/ \/_/   \/_____/
                                                                                      
  [bold cyan]Rule34[/bold cyan] | [cornflower_blue]OnlyFans[/cornflower_blue] | [dodger_blue2]Fansly[/dodger_blue2] | [salmon1]Patreon[/salmon1] | [dark_cyan]SubscribeStar[/dark_cyan] | [deep_sky_blue4]E621[/deep_sky_blue4] | [sky_blue1]Fanbox[/sky_blue1] | [hot_pink]Gumroad[/hot_pink]
  by [light_coral]https://github.com/Hyphonic
  [white]Version 3.0.1


'''

############################################################
#                                                          #
#                           Main                           #
#                                                          #
############################################################

def Main():
    Console(force_terminal=True).print(Screen)
    try:
        InitialGlobalLimit = Config['global_limit']

        CheckForDuplicateIds()

        # Set creator_limit to 0 if platform not in enabled_platforms
        for Platform in Config['directory_names'].keys():
            if not Config['enabled_platforms'][Platform]:
                Config[Platform]['creator_limit'] = 0

        FavoriteFetcher('coomer')  # Fetch favorites from Coomer
        FavoriteFetcher('kemono')  # Fetch favorites from Kemono

        # Ensure Config['cached_hashes'] is available
        Config['cached_hashes'] = DownloadManager([], None).CachedHashes

        for Platform in Config['directory_names'].keys():
            if Platform in ['rule34', 'e621']:
                for Creator in Config[Platform]['ids']:
                    Config[Platform]['directory_names'].append(f'{Config["directory_names"][Platform]}/{Creator.capitalize()}')
                    Config[Platform]['names'].append(Creator.capitalize())
            else:
                for Creator in Config[Platform]['names']:
                    Config[Platform]['directory_names'].append(f'{Config["directory_names"][Platform]}/{Creator.capitalize()}')
        
        with open('config.json', 'w', encoding='utf-8') as ConfigFile:
            json.dump(Config, ConfigFile, indent=4, ensure_ascii=False)
            Logger.Debug('Generated config.json')

        for Platform in Config['directory_names'].keys():
            Logger.Debug(f'∙ Loaded {len(Config[Platform]["ids"])} {Config["platform_names"][Platform]} creators')
        Logger.Info(f'Loaded {sum([len(Config[Platform]["ids"]) for Platform in Config["directory_names"].keys() if Config[Platform]["creator_limit"] > 0])} creators')

        Logger.Debug('∙ Starting in dry-run mode') if Config.get('dry_run', False) else None

        # Create progress for each enabled platform
        ProgressColumns = [
            TextColumn("[cyan]{task.fields[platform]}"),
            BarColumn(bar_width=40),
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

        with Progress(*ProgressColumns, console=Console(force_terminal=True), auto_refresh=False) as ProgressBar:
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
                        
                        Config['global_limit'], Result = Fetcher(
                            Platform=Platform,
                            Id=Id,
                            Name=Name,
                            DirectoryName=DirectoryName,
                            CachedHashes=Config['cached_hashes'],
                            CreatorLimit=Config[Platform]['creator_limit'],
                            GlobalLimit=Config['global_limit']
                        ).Scrape()
                        Collection.append(Result)
                        ProgressBar.refresh()
        
        if Collection:
            AllFiles = []
            for Result in Collection:
                for Platform in Result:
                    for Creator in Result[Platform]:
                        AllFiles.extend([(FileData, Platform, Creator) for FileData in Result[Platform][Creator]])
                        
            if AllFiles:
                # Use DownloadManager instead of separate functions
                DownloadManager(AllFiles, None).Download()

    except KeyboardInterrupt:
        Logger.Warning("Interrupted by user")
    except Exception as e:
        Logger.Error(f"Error: {e}")

if __name__ == '__main__':
    Main()