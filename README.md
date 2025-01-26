# 🌟 NeoFans

A modern and efficient content fetcher, revamped from the now deprecated OmniFans. NeoFans helps you download and organize content from various creator platforms.

## ✨ Features

### 🎯 Supported Platforms

- 💗 [Gumroad](https://gumroad.com/) - Digital content marketplace
- 🅿️ [Patreon](https://www.patreon.com/) - Creator subscription platform  
- ⭐ [Subscribestar](https://www.subscribestar.com/) - Content creator support platform
- 🌀 [OnlyFans](https://onlyfans.com/) - Content subscription service
- 📦 [Fanbox](https://fanbox.cc/) - Japanese creator platform
- 🔒 [Fansly](https://fansly.com/) - Content creator platform
- 🎨 [Rule34](https://rule34.xxx/) - Art repository
- 🐾 [E621](https://e621.net/) - Art repository
- ~🔮 [Reddit](https://reddit.com/) - Reddit~

> [!NOTE]
> Reddit support has been removed due to the platform's restrictions. You may suggest new platforms by opening an issue.

### 🚀 Key Features

- **Smart Fetching**: Downloads content until reaching creator or global limits
- **Performance Optimized**: Multi-threaded downloads with configurable workers
- **Cache System**: Tracks downloaded files to avoid duplicates
- **Progress Tracking**: Rich console UI showing download progress
- **Proxy Support**: Built-in proxy checker and rotator
- **Format Support**: Handles various media formats

### ⚙️ Technical Features

- Asynchronous downloads using `aiohttp` and `aiofiles`
- Configurable download limits per creator and globally
- File hash tracking to prevent duplicate downloads
- Progress bars with detailed statistics
- Error handling and retry mechanisms

## 🛠️ Installation

```bash
git clone https://github.com/Hyphonic/NeoFans.git
cd NeoFans
pip install -r requirements.txt
```

To allow GitHub Actions to run the workflow, you need to add the following secrets to your repository:

- `COOMER_SESS`: [Coomer Session Cookie](https://coomer.su/)
- `KEMONO_SESS`: [Kemono Session Cookie](https://kemono.su/)

I mainly use these websites to fetch the favorites of the creators. If you don't want to use this program on GitHub Actions, you can either import the session token as an environment variable or change the code to use your session token depending on the platform. (<- Offline use only)

> [!NOTE]
> Since version 2.1.0, NeoFans uses the Rich library for console output.

## 🚀 Usage

```bash
python Main.py
```

> This repository comes with a workflow that is readily available for use. Keep in mind that some visual errors may occur since GitHub Actions doesnt allow for line refreshing programs such as the rich progress bars.

## 📚 Config Usage

NeoFans uses a built-in config system that allows you to customize the behavior of the program.
<details>
  <summary>Click here to see the config structure</summary>

```json
[
  {
    "platform": {
      "creator_limit": 1000,
      "ids": [],
      "names": [],
      "directory_names": []
    },
    "directory_names": {
      "platform": "🎨 Platform Name"
    },
    "platform_names": {
      "platform": "[rich color]Platform Name[/rich color]"
    },
    "threads": {
      "max_workers": 32
    },
    "enabled_platforms": {
      "platform": true
    },
    "global_limit": 500,
    "dry_run": false,
    "platform_limit_debug": 1
  }
]
```

</details>

## 📝 Credits

- <kbd>[Jesewe's Proxy Checker (Modified version)](https://github.com/Jesewe/proxy-checker)</kbd>
