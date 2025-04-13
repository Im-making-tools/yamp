import asyncio
import concurrent.futures
import json
import re
import sys
import time
import urllib
from collections import OrderedDict
from contextlib import ExitStack
import tomllib
import os
from datetime import datetime
from pathlib import Path
import logging
import subprocess
import lzma
from urllib.parse import urlparse

picomc_source = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'picomc', 'src')
if os.path.exists(picomc_source):  # For debugging
    sys.path.insert(0, picomc_source)

from picomc.version import VersionManager
from picomc.launcher import Launcher
from picomc.account import AccountManager, OfflineAccount
from picomc.instance import InstanceManager, Instance
from picomc.downloader import DownloadQueue as OriginalDownloadQueue
from picomc.mod import forge, fabric
from picomc.utils import Directory
from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table
from rich.text import Text
import rich.progress as progress

import xxhash
import requests
import aiohttp
import aiofiles

console = Console(highlighter=None)

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True, tracebacks_show_locals=False, show_time=False, show_path=False, markup=True)],
)
logging.getLogger("urllib3").setLevel(logging.ERROR)
JAVA = shutil.which("java")

url_to_name = {}
class DownloadQueue:
    chunk_size = 1024 * 64
    def __init__(self):
        self.q = []
        self.size = 0
        self.sem = asyncio.Semaphore(5)
        self.prog = progress.Progress(
                progress.TaskProgressColumn(),
                progress.BarColumn(),
                progress.DownloadColumn(binary_units=True),
                progress.TransferSpeedColumn(),
                progress.TimeRemainingColumn(),
                console=console,
        )

    def add(self, url, filename, size=None):
        self.q.append((url, filename))
        if self.size is not None and size is not None:
            self.size += size
        else:
            self.size = None

    def __len__(self):
        return len(self.q)

    async def _download_one(self, task, session: aiohttp.ClientSession, url, filename):
        async with self.sem:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise ValueError(f"Download failed, server returned code={resp.status} url={url}")
                async with aiofiles.open(filename, mode='wb') as f:
                    async for data in resp.content.iter_chunked(self.chunk_size):
                        await f.write(data)
                        self.prog.update(task, advance=len(data))
            if url in url_to_name:
                rid, filename = url_to_name[url]
                MainLauncher.log.info(f"Downloaded [yellow]{rid} [gray50]{filename}")

    async def async_download(self):
        async with aiohttp.ClientSession() as session:
            with self.prog:
                task = self.prog.add_task(description="Downloading", total=self.size)
                if self.size is None:
                    self.size = 0
                    for url, filename in self.q:
                        async with self.sem, session.head(url) as resp:
                            self.size += int(resp.headers.get('content-length', 0))
                    self.prog.update(task, total=self.size)
                tasks = []
                for url, filename in self.q:
                    tasks.append(self._download_one(task, session, url, filename))
                await asyncio.gather(*tasks)
                return True

    def download(self):
        if not self.q:
            return True
        return asyncio.run(self.async_download())


def download_override(self):
    q = DownloadQueue()
    q.q = self.q
    q.size = self.size
    return q.download()

OriginalDownloadQueue.download = download_override

def get_manifest_override(self):
    from picomc.logging import logger

    manifest_filepath = self.launcher.get_path(Directory.VERSIONS, "manifest.json")
    try:
        m = requests.get(self.MANIFEST_URL, timeout=1).json()  # this takes forever!
        with open(manifest_filepath, "w") as mfile:
            json.dump(m, mfile, indent=4, sort_keys=True)
        return m
    except requests.ConnectionError or requests.exceptions.ReadTimeout:
        logger.warning(
            "Failed to retrieve version_manifest. "
            "Check your internet connection."
        )
        try:
            with open(manifest_filepath) as mfile:
                logger.warning("Using cached version_manifest.")
                return json.load(mfile)
        except FileNotFoundError:
            logger.warning("Cached version manifest not available.")
            raise RuntimeError("Failed to retrieve version manifest.")

VersionManager.get_manifest = get_manifest_override

def load_json_xz(filepath: Path, default_factor=dict):
    try:
        if filepath.exists():
            return json.loads(lzma.open(filepath, 'rb').read())
        elif filepath.name.endswith('.xz'):
            uncompressed_file = filepath.parent / filepath.name[:-3]
            if uncompressed_file.exists():
                return json.loads(uncompressed_file.read_bytes())
    except json.decoder.JSONDecodeError:
        MainLauncher.log.error(f"Failed to decode JSON in {filepath} ignoring file")
    return default_factor()

def json_encoder(obj):
    if isinstance(obj, set):  # dont write sets
        return None
    raise TypeError(f'Cannot serialize object of {type(obj)}')


def save_json_xz(filepath: Path, data: dict):
    os.makedirs(filepath.parent, exist_ok=True)
    data = json.dumps(data, default=json_encoder)
    with lzma.open(filepath, 'wt') as f:
        f.write(data)


class MainLauncher:
    log = logging.getLogger('launcher')

    def add_resources(self, cfg, typ):
        for modid, options in cfg.items():
            if modid.isdigit():
                source = 'curseforge'
            # method below not recommended, seems like its not always reliable
            elif modid.startswith('curse-') or modid.startswith('curseforge-'):
                modid = modid.split('-', 1)[1]
                source = 'curseforge'
            elif 'url' in options:
                source = 'url'
            else:
                source = 'modrinth'
            self.RESOURCES[f'{source}-{modid}'] = (modid, source, typ, options)


    def __init__(self, config_file: str, root_dir: str|None = None, java: str|None = None):
        self.session = requests.Session()
        if not root_dir:
            if os.name == 'nt':
                root_dir = os.environ.get("YAMP_ROOT", "C:/Games/Minecraft")
            else:
                root_dir = os.environ.get("YAMP_ROOT", "~/Games/Minecraft")
        self.MINECRAFT_DIR = Path(root_dir).expanduser()
        self.DOWNLOADS = self.MINECRAFT_DIR / 'downloads'

        if config_file.startswith('http'):
            res = self.session.get(config_file)
            res.raise_for_status()
            config_data = res.text
        else:
            config_data = Path(config_file).read_text()
        config = tomllib.loads(config_data)
        self.MC_VERSION: str = config['minecraft']['version']
        self.LOADER: str = config['minecraft']['loader'].lower()
        self.LOADER_VER: str = config['minecraft']['loader_ver']
        self.VERSION: str = f"{self.MC_VERSION}-{self.LOADER}-{self.LOADER_VER}"
        self.PACK_NAME: str = config['minecraft']['pack_name']
        self.RESOURCES = {}
        self.add_resources(config.get('mods', {}), 'mods')
        self.add_resources(config.get('resourcepacks', {}), 'resourcepacks')
        self.add_resources(config.get('shaders', {}), 'shaderpacks')

        self.SOURCE_MAP = {
            'curseforge': self._fetch_curseforge_resource,
            'modrinth': self._fetch_modrinth_resource,
            'url': self._fetch_url_resource,
            'file': self._fetch_url_resource,
        }

        self.es = ExitStack()
        self.es.__enter__()
        self.launcher = Launcher(self.es, root=self.MINECRAFT_DIR / "picomc")
        self.am = AccountManager(self.launcher)
        self.config = config
        self.inst = self.get_instance(java)
        self.cached = {}
        self.db = {'_loaded': False}
        self.old_files = set()
        self.DOWNLOADS.mkdir(parents=True, exist_ok=True)
        self.mapping = {}  # mod mapping

    def setup_loader(self):
        loader_dir = self.launcher.get_path(Directory.VERSIONS) / self.VERSION
        if not loader_dir.exists():
            self.log.info(f"Installing {self.LOADER} to {loader_dir}")
            if self.LOADER == 'fabric':
                fabric.install(
                    self.launcher.get_path(Directory.VERSIONS),
                    self.MC_VERSION,
                    self.LOADER_VER,
                    version_name=self.VERSION,
                )
            elif self.LOADER == 'forge':
                forge.install(
                    self.launcher.get_path(Directory.VERSIONS),
                    self.launcher.get_path(Directory.LIBRARIES),
                    self.MC_VERSION,
                    version_name=self.LOADER_VER,
                )
            else:
                raise ValueError(f"Unsupported loader: {self.LOADER}, should be either forge or fabric")


    def list_loader_versions(self):
        if self.LOADER == 'fabric':
            obj = requests.get(f"https://meta.fabricmc.net/v2/versions/loader/{urllib.parse.quote(self.MC_VERSION)}").json()
            self.log.info(f"Recent [yellow]fabric {self.MC_VERSION}[/yellow] versions:")
            for loader in reversed(obj):
                stable = "[green]yes" if loader["loader"]["stable"] else "[red]no"
                console.print(f'[gray50] * [yellow]{loader["loader"]["version"]}[/yellow]\tstable={stable}')
        elif self.LOADER == 'forge':
            obj = requests.get("https://mrnavastar.github.io/ForgeVersionAPI/forge-versions.json").json()
            if self.MC_VERSION not in obj:
                self.log.error(f"No avaialbe [yellow]forge {self.MC_VERSION}[/yellow] versions!")
                return
            self.log.info(f"Recent [yellow]forge {self.MC_VERSION}[/yellow] versions:")
            for loader in reversed(obj[self.MC_VERSION]):
                console.print(f'[gray50] * [yellow]{loader["id"]}[/yellow]\tstatus=[bright_blue]{loader["type"]}')


    def get_instance(self, java) -> Instance:
        im = InstanceManager(self.launcher)
        if not im.exists(self.PACK_NAME):
            inst = im.create(self.PACK_NAME, self.VERSION)
        else:
            inst = im.get(self.PACK_NAME)
        if 'java_max_memory' in self.config['minecraft']:
            inst.config['java.memory.max'] = self.config['minecraft']['java_max_memory']
        if java:
            inst.config['java.path'] = str(java)
        inst.config['version'] = self.VERSION
        return inst

    def _fetch_url_resource(self, modid, typ, name=None, url=None, **kw):
        if url is None:
            self.log.warning(f"URL source [yellow]{modid}[/yellow] requires url parameter")
            return
        url_parts = urlparse(url)

        data = {
            'response': None, 'source': 'url', 'type': typ, 'dependencies': {},
            'last_checked': time.time(), 'rid': f"url-{xxhash.xxh32_hexdigest(url)}",
        }
        if url_parts.scheme == 'file':
            data['latest_file'] = {
                    'filename': modid,
                    'url': url,
                    'size': Path(url_parts.path).stat().st_size,
                    'hash': xxhash.xxh32_hexdigest(url)
            }
            data['name'] = f"{modid} (local file)"
        else:
            res = self.session.head(url)
            res.raise_for_status()
            data['latest_file'] = {
                    'filename': modid,
                    'url': url,
                    'size': int(res.headers['content-length']),
                    'hash': res.headers.get('etag', '').strip('"') or xxhash.xxh32_hexdigest(url)
            }
            data['name'] = f"{modid} ({url_parts.hostname})"
        return data


    def _fetch_curseforge_resource(self, modid, typ, any_version=False, **options):
        params = {}
        if not any_version:
            params['version'] = self.MC_VERSION
        if typ == "mods":
            params['loader'] = options.get('loader', self.LOADER)  # in case cross-loader mods used
        res = self.session.get(f'https://api.cfwidget.com/minecraft/mc-mods/{modid}', params=params)
        res.raise_for_status()
        data = {'response': res.json(), 'source': 'curseforge', 'type': typ, 'last_checked': time.time()}
        if 'download' in data['response']:
            file = data['response']['download']
        else:
            files = sorted(filter(lambda x: self.MC_VERSION in x['versions'] and params['loader'].capitalize() in x['versions'], data['response']['files']), key=lambda x: -x['id'])
            if len(files) == 0:
                raise ValueError(f"No versions available for {data['response']['title']} {modid} ({data['response']['url']['curseforge']})")
            file = files[0]
        sid = str(file['id'])
        data['latest_file'] = {
            'filename': file['name'],
            'url': f'https://mediafilez.forgecdn.net/files/{sid[:-3]}/{sid[-3:]}/{file['name']}',
            'size': file['filesize'],
        }
        # res = self.session.head(data['latest_file']['url'])
        data['latest_file']['hash'] = xxhash.xxh32_hexdigest(data['latest_file']['url'])
        data['name'] = f"{data['response']['title']} ({file['display']})"
        data['rid'] = f"curseforge-{data['response']['id']}"
        data['dependencies'] = {}
        return data

    # def get_mod_info(self):
    #     mod_ids = [data['response']['project_id'] for data in self.cached.values() if data['used'] and data['source'] == 'modrinth']
    #     if len(mod_ids) > 0:
    #         res = self.session.get("https://api.modrinth.com/v2/projects", params={'ids': json.dumps(mod_ids)}, timeout=2)
    #         res.raise_for_status()
    #         docs = {hash(f'modrinth-project-{proj["id"]}'): proj for proj in res.json()}
    #         doc_ids = set(docs.keys())
    #         new_ids = doc_ids.difference({d.doc_id for d in self.db.get(doc_ids=list(doc_ids))})
    #         old_ids = doc_ids.difference(new_ids)
    #         self.db.remove(doc_ids=list(old_ids))
    #         extras = {'_source': 'modrinth', '_doc_type': 'project'}
    #         self.db.insert_multiple([Document(dict(**docs[did], **extras), did) for did in doc_ids])

    def find_mod_info(self, mod_id):
        mod = self.cached[mod_id]
        resp = mod.get('response', {})
        proj_id = resp.get('project_id', resp.get('id', ''))
        value = dict(_source=mod['source'], _doc_type='project')
        if not self.db['_loaded']:
            self.db = load_json_xz(self.MINECRAFT_DIR / 'project_cache.xz', dict)
            self.db['_loaded'] = True
        if mod_id in self.db:
            return self.db[mod_id]
        if mod['source'] == 'modrinth':
            res = self.session.get(f"https://api.modrinth.com/v2/project/{proj_id}", timeout=5)
            res.raise_for_status()
            value.update(**res.json())
        elif mod['source'] == 'curseforge':
            value.update(**resp)
        self.db[mod_id] = value
        return value

    def save_mod_info(self):
        save_json_xz(self.MINECRAFT_DIR / 'project_cache.xz', self.db)

    def _fetch_modrinth_resource(self, modid, typ, any_version=False, version_id=None, version_number=None, **options):
        params = {}
        if not any_version:
            params['game_versions'] = json.dumps([self.MC_VERSION])
        if typ == "mods":
            params['loaders'] = json.dumps([options.get('loader', self.LOADER)])
        res = self.session.get(f'https://api.modrinth.com/v2/project/{modid}/version', params=params, timeout=5)
        res.raise_for_status()
        res_data = res.json()
        if len(res_data) == 0:
            res2 = self.session.get(f'https://api.modrinth.com/v2/project/{modid}', params=params, timeout=5).json()
            raise ValueError(f"No versions available for {res2['title']} {modid} (https://modrinth.com/mod/{res2['slug']})")
        response = res_data[0]
        if version_id is not None:
            response = next(filter(lambda x: x['id'] == version_id, response))
        elif version_number is not None:
            response = next(filter(lambda x: x['version_number'] == version_number, response))
        data = {'response': response, 'source': 'modrinth', 'type': typ, 'last_checked': time.time()}
        data['rid'] = f"modrinth-{data['response']['project_id']}"
        if len(data['response']['files']) == 1:
            data['latest_file'] = data['response']['files'][0]
        else:
            data['latest_file'] = next(filter(lambda x: x['primary'], data['response']['files']))
        data['name'] = data['response']['name']
        data['latest_file']['hash'] = data['latest_file']['hashes']['sha512'][:64]

        data['dependencies'] = {
            f"modrinth-{dep['project_id']}": (dep['project_id'], 'modrinth', 'mods', {})
            for dep in data['response']['dependencies']
            if dep['dependency_type'] == 'required'
        } if typ == 'mods' else {}
        return data

    def fetch_resources(self, resources, cache, prog, message='Fetching projects', check_update=False, requested_by='config'):
        downloads = []
        task = prog.add_task(message, total=len(resources), id='')
        error = False
        added = set()
        for rid, values in resources.items():
            prog.update(task, id=rid)
            if len(values) == 2:
                values, requested_by = values
            if len(values) == 3:
                modid, source, typ = values
                opt = {}
            else:
                modid, source, typ, opt = values
            resolved_rid = self.mapping.get(rid, rid)
            data = cache.get(resolved_rid, self.cached.get(resolved_rid, {}))
            version_match = True

            # Just check if version/loader still match what's in cache (in case mc version is changed in .toml)
            if 'response' in data and 'source' in data:
                local_loader = opt.get('loader', self.LOADER)
                if data['source'] == 'modrinth':
                    version_match &= opt.get('incompatible', False) or self.MC_VERSION in data['response']['game_versions']
                    version_match &= data['type'] != 'mods' or local_loader in data['response']['loaders']
                elif data['source'] == 'curseforge':
                    versions = data['response']['files']
                    if not opt.get('incompatible', False):
                        versions = filter(
                            lambda x: self.MC_VERSION in x['versions'] and (
                                    data['type'] != 'mods' or local_loader.capitalize() in x['versions']
                            ), data['response']['files']
                        )
                    files = list(sorted(versions, key=lambda x: -x['id']))  # get latest
                    if len(files) == 0:
                        version_match = False
                    else:
                        sid = str(files[0]['id'])
                        data['latest_file'] = {
                            'filename': files[0]['name'],
                            'url': f'https://mediafilez.forgecdn.net/files/{sid[:-3]}/{sid[-3:]}/{files[0]['name']}',
                            'size': files[0]['filesize'],
                        }
                        data['latest_file']['hash'] = xxhash.xxh32_hexdigest(data['latest_file']['url'])
            if 'type' not in data or check_update or not version_match:
                try:
                    data = self.SOURCE_MAP[source](modid, typ, **opt)
                except requests.exceptions.ReadTimeout:
                    self.log.error(f"Fetching [gray50]{rid}[/gray50] timed out")
                    error = True
                except requests.exceptions.HTTPError as e:
                    self.log.error(f"Fetching [gray50]{rid}[/gray50] returned server status {e.response.status_code}")
                    error = True
                except ValueError as e:
                    req_str = requested_by
                    if requested_by in cache:
                        req_str += f" ({cache[requested_by]['latest_file']['filename']})"
                    self.log.error(f"{e.args[0]}, requested by {req_str}")
                    error = True
            if 'rid' not in data:
                continue
            if data['rid'] not in cache or 'requested_by' not in data:
                data['requested_by'] = set(requested_by)
                data['options'] = opt
            else:
                data['requested_by'].add(requested_by)
                data['options'].update(opt)
            data['used'] = True
            data['disallow'] = opt.get('disallow', False)
            filename: Path = self.inst.directory / 'minecraft' / data['type'] / data['latest_file']['filename']
            cache[data['rid']] = data
            self.mapping[rid] = data['rid']
            if not filename.exists() and not data['disallow']:
                url_to_name[data['latest_file']['url']] = data['rid'], data['latest_file']['filename']
                downloads.append((data['latest_file']['url'], filename, data['latest_file']['size'], data['latest_file']['hash']))
            if not data['disallow']:
                added.add(data['rid'])
            prog.update(task, advance=1)

        dependencies = {}
        for rid in added:
            proj = cache[rid]
            if not proj.get('used', False):
               continue
            if proj['options'].get('ignore_dep'):
                continue
            for drid, dval in proj['dependencies'].items():
                dependencies[drid] = (dval, rid)
        # dependencies = {drid:(dval, proj) for proj in cache.values() for drid, dval in proj['dependencies'].items() if proj.get('used', False)}
        if len(dependencies) > 0:
            cache, downloads_dep, error_dep = self.fetch_resources(dependencies, cache, prog, message="Fetching dependencies", check_update=check_update)
            downloads += downloads_dep
            error |= error_dep
        return cache, downloads, error

    def setup_files(self, update=False):
        # Fetching all dependencies
        self.old_files = {file.name for file in (self.inst.directory / 'minecraft' / 'mods').glob('*.jar')}
        modlist_file: Path = self.inst.directory / 'minecraft' / 'mods' / '.mod_list.xz'
        cached = load_json_xz(modlist_file)
        self.cached = cached['_mod_dict'] if '_mod_dict' in cached else cached
        self.mapping = cached.get('_mapping', {})

        for k, v in self.cached.items():
            self.cached[k]['used'] = False  # Track which projects are used this time
        with progress.Progress(
            progress.TextColumn("[progress.description]{task.description}"),
            progress.BarColumn(),
            progress.MofNCompleteColumn(),
            progress.TimeRemainingColumn(),
            progress.TextColumn("[gray50]{task.fields[id]}"),
            console=console,
        ) as prog:
            self.cached, downloads, error = self.fetch_resources(self.RESOURCES, {}, prog, check_update=update)
        save_json_xz(modlist_file, {'_mod_dict': self.cached, '_mapping': self.mapping})
        if error:
            raise Exception("Stopping because of previous errors")
        if len(downloads) > 0:
            dq = DownloadQueue()

            # Removes duplicates
            downloads = {hash: (url, fname, size) for url, fname, size, hash in downloads}
            for hash, (url, fname, size) in downloads.items():
                if (self.DOWNLOADS / hash / fname.name).exists() or url.startswith('file://'):
                    continue
                (self.DOWNLOADS / hash).mkdir(exist_ok=True)
                dq.add(url, self.DOWNLOADS / hash / fname.name, size)
            self.log.info("Downloading [blue]mods[/blue]..")
            dq.download()
            for hash, (url, fname, size) in downloads.items():
                fname.parent.mkdir(parents=True, exist_ok=True)
                if url.startswith('file://'):
                    os.symlink(url[7:], fname)
                else:
                    os.symlink(self.DOWNLOADS / hash / fname.name, fname)
        for mod in self.cached.values():
            if not mod['used'] or mod['type'] != 'mods' or mod['disallow']:
                continue
            if mod['latest_file']['filename'] in self.old_files:
                self.old_files.remove(mod['latest_file']['filename'])
        self.update_custom_files()

    def remove_old_mods(self):
        for old_file in self.old_files:
            self.log.warning(f"Removing mod file [yellow]{old_file}")
            os.remove(self.inst.directory / 'minecraft' / 'mods' / old_file)

    def list_mods_files(self):
        mod_files = sorted([file.name for file in (self.inst.directory / 'minecraft' / 'mods').glob('*.jar')])
        existing = {m['latest_file']['filename']: m for m in self.cached.values()}
        dependents = {}

        with progress.Progress(
            progress.TextColumn("[progress.description]{task.description}"),
            progress.BarColumn(),
            progress.MofNCompleteColumn(),
            progress.TimeRemainingColumn(),
            progress.TextColumn("[gray50]{task.fields[id]}"),
            console=console
        ) as prog:
            items = [(mod, d) for mod in existing.values() for d in mod['dependencies'].values()]
            task = prog.add_task('Fetching project info', total=len(items), id='')
            for mod, d in items:
                if d[0] not in dependents:
                    dependents[d[0]] = []
                prog.update(task, id=mod['rid'])
                qr = self.find_mod_info(mod['rid'])
                t = f'[{mod['rid']}] {qr['title']}'
                dependents[d[0]].append(t)
                prog.update(task, advance=1)

        table = Table()
        table.add_column("File name", style="yellow")
        table.add_column("Name")
        table.add_column("Updated")
        table.add_column("Client")
        table.add_column("Server")
        table.add_column("Required by")
        table.add_column("RID")

        unknown = Text('?', style="bold red")
        for mod_file in mod_files:
            if mod_file not in existing:
                table.add_row(mod_file, unknown, unknown, unknown, unknown, unknown, unknown)
                continue
            mod = existing[mod_file]
            qr = self.find_mod_info(mod['rid'])
            mod_name = qr.get('title', unknown)
            updated = datetime.fromisoformat(qr['updated']).strftime('%Y/%m/%d') if 'updated' in qr else unknown
            server_side = qr.get('server_side', Text('yes?', style="bright_yellow"))
            if 'server_side' in mod['options']:
                server_side = Text('enabled' if mod['options']['server_side'] else 'disabled', style="bright_yellow")
            client_side = qr.get('client_side', Text('yes?', style="bright_yellow"))
            proj_id = mod.get('response', {}).get('project_id', '')
            dep = '\n'.join(dependents.get(proj_id, []))
            table.add_row(mod_file, mod_name, updated, client_side, server_side, dep, mod['rid'])
        console.print(table)

    def update_custom_files(self, root=None):
        for filename, options in self.config.get('custom', {}).items():
            if root is None:
                root = self.inst.directory / 'minecraft'
            filepath: Path = root.joinpath(filename).resolve()
            if Path(os.path.commonpath([root, filepath])) != root:
                self.log.warning(f"File {filename} ({filepath}) is outside minecraft directory!")
                continue
            file_data = None
            if 'content' in options:
                file_data = options['content']
            elif 'json' in options:
                if filepath.exists() and options.get('json_update', False):
                    exising_data = json.loads(filepath.read_bytes())
                    if isinstance(exising_data, dict):
                        exising_data.update(options['json'])
                else:
                    exising_data = options['json']
                file_data = json.dumps(exising_data)
            if file_data is not None:
                current_hash = xxhash.xxh64(filepath.read_bytes().strip() if filepath.exists() else b'').digest()
                update_hash = xxhash.xxh64(file_data.encode().strip()).digest()
                if current_hash != update_hash:
                    self.log.info(f"Updating file [yellow]{filename}")
                    os.makedirs(filepath.parent, exist_ok=True)
                    filepath.write_text(file_data)

    def force_resource_packs(self):
        resource = {k: self.cached[self.mapping[k]] for k in self.RESOURCES.keys() if self.RESOURCES[k][2] == 'resourcepacks'}
        if len(resource) == 0:
            return

        options_file: Path = self.inst.directory / 'minecraft' / 'options.txt'
        if not options_file.exists():
            packs = ['file/' + r['latest_file']['filename'] for r in resource.values()]
            options_file.write_text(f'resourcePacks:{json.dumps(packs)}')
            return
        lines = []
        options = OrderedDict()
        for line in options_file.read_text().splitlines():
            key = line.split(':')[0]
            value = ':'.join(line.split(':')[1:])
            options[key] = value

        packs = json.loads(options['resourcePacks'])
        packs2 = json.loads(options['incompatibleResourcePacks'])
        packs_f = [p for p in packs if not p.startswith('file/')]
        packs_f2 = [p for p in packs2 if not p.startswith('file/')]
        added = False
        for r in resource.values():
            name = 'file/' + r['latest_file']['filename']
            packs_f.append(name)
            if r['options'].get('incompatible', False):
                packs_f2.append(name)
            if name not in packs:
                added = True
        if not added:
            return
        options['resourcePacks'] = json.dumps(packs_f)
        options['incompatibleResourcePacks'] = json.dumps(packs_f2)
        self.log.info(f"Updating option [yellow]{options['resourcePacks']}")
        options_file.write_text('\n'.join([f'{k}:{v}' for k, v in options.items()]))
        #     resourcePacks:["vanilla","mod_resources","Moonlight Mods Dynamic Assets","firmalife_data","file/Serified Font v1.1 f1-34.zip"]

    def start_server(self, directory: Path | str | None = None):
        server_dir: Path = self.inst.directory / 'server' if directory is None else Path(directory)
        server_dir.mkdir(exist_ok=True, parents=True)
        if not (server_dir / 'libraries').exists():
            os.symlink(self.launcher.root / 'libraries', server_dir / 'libraries')
        if not (server_dir / 'run.sh').exists():
            if self.LOADER == 'forge':
                ver = f'{self.MC_VERSION}-{self.LOADER_VER}'
                h = self.session.head(f'https://maven.minecraftforge.net/net/minecraftforge/forge/{ver}/forge-{ver}-installer.jar')
            elif self.LOADER == 'fabric':
                raise NotImplemented  # not tested. Probably will work out of the box
                # res = self.session.get('https://mcutils.com/api/server-jars/fabric')
            else:
                raise ValueError(f'Unsupported loader: {self.LOADER}')
            h.raise_for_status()
            installer_file = self.DOWNLOADS / 'installers' / re.findall("filename=\"?([^\"]+)\"?", h.headers['content-disposition'])[0]
            dq = DownloadQueue()
            dq.add(h.url, installer_file, int(h.headers['content-length']))
            self.log.info(f"Downloading [blue]server installer[/blue].. [gray50]{installer_file}")
            dq.download()
            self.log.info("Running [blue]server installer[/blue]..")
            subprocess.run([self.inst.get_java(), '-jar', str(installer_file), '--installServer'], cwd=server_dir)
        if not (server_dir / 'eula.txt').exists():
            (server_dir / 'eula.txt').write_text('eula=TRUE\n')
        if not (server_dir / 'server.properties').exists():
            fields = [f'{key}={value}' for key, value in self.config.get('server', {}).items()]
            (server_dir / 'server.properties').write_text('\n'.join(fields))
        (server_dir / 'mods').mkdir(exist_ok=True)
        old_files = {file.name for file in (server_dir / 'mods').glob('*.jar')}
        added = set()
        for mod in self.cached.values():
            if not mod['used'] or mod['type'] != 'mods' or mod['disallow'] or mod['options'].get('client_only', False):
                continue
            if mod['rid'] in added:
                continue
            info = self.find_mod_info(mod['rid'])
            added.add(mod['rid'])
            hash = mod['latest_file']['hash']
            url = mod['latest_file']['url']
            fname = mod['latest_file']['filename']
            trunc_title = info.get('title', 'None')
            if len(trunc_title) > 30:
                trunc_title = trunc_title[:28] + '..'

            if not mod['options'].get('server_side', info.get('server_side') != 'unsupported'):
                self.log.warning(f"Unsupported: [bright_red]{trunc_title:30}\t[yellow]{fname}[/yellow]")
                continue
            dest_file: Path = server_dir / 'mods' / fname
            src_file = self.DOWNLOADS / hash / fname
            self.log.info(f"Adding mod: [bright_blue]{trunc_title:30}\t[yellow]{fname}[/yellow]")
            if url.startswith('file://'):
                src_file = Path(url[7:])
            if dest_file.exists():
                if dest_file.readlink() == src_file:
                    if fname in old_files:
                        old_files.remove(fname)
                    continue
                self.log.warning(f"Removing old symlink file [yellow]{dest_file.name}")
                dest_file.unlink()
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(src_file, dest_file)
            if fname in old_files:
                old_files.remove(fname)
        self.save_mod_info()
        for old_file in old_files:
            self.log.warning(f"Removing mod file [yellow]{old_file}")
            os.remove(server_dir / 'mods' / old_file)
        self.update_custom_files(server_dir)
        srv_args = [self.inst.get_java()]
        if 'java_max_memory' in self.config['minecraft']:
            srv_args += [f"-Xmx{self.config['minecraft']['java_max_memory']}"]
        srv_args += ['@user_jvm_args.txt', f'@libraries/net/minecraftforge/forge/{self.MC_VERSION}-{self.LOADER_VER}/unix_args.txt', '-nogui']
        self.log.info(f"[bright_blue]Starting server: [gray50]{' '.join(srv_args)}")
        subprocess.run(srv_args, cwd=server_dir)


    def get_account(self, name):
        self.log.info("Available accounts: [blue]" + (' '.join(self.am.list()) if self.am.list() else '[red bold]NONE'))
        if self.am.config.get('default') is None or name is not None:
            if name is None:
                name = input("Your minecraft username: ").strip()
                if len(name) == 0:
                    exit(1)
            if name in self.am.list():
                acc = self.am.get(name)
            else:
                acc = OfflineAccount.new(self.am, name)
                self.am.add(acc)
            self.am.set_default(acc)
            self.am.get_default()
            return acc
        return self.am.get_default()

    def start_client(self, account_name=None):
        self.log.info(f"Launching modpack [yellow]{self.PACK_NAME}")
        self.inst.launch(self.get_account(account_name))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog='YAMP', description='Yet Another Minecraft Packmaker. Download and run Minecraft modpacks')
    parser.add_argument('-u', '--update', action='store_true', help='Check for updates and download latest')
    parser.add_argument('-a', '--account', default=None, help='Minecraft username to use')
    parser.add_argument('--java', default=None, help='Specify java path')
    parser.add_argument('pack_file', help='Modpack toml filename or url')
    parser.add_argument('action', choices=['client', 'check', 'server', 'loader_vers', 'dryrun'], help='Specify action to do')

    args = parser.parse_args()

    import picomc.downloader
    import mock
    with mock.patch.object(picomc.downloader, 'DownloadQueue', DownloadQueue):
        ml = MainLauncher(args.pack_file, java=args.java)
        ml.log.setLevel(logging.INFO)
        if args.action == 'loader_vers':
            ml.list_loader_versions()
            exit()
        ml.setup_files(args.update)
        if args.action == 'check':
            ml.list_mods_files()
            ml.save_mod_info()
            exit()
        if args.action == 'server':
            ml.start_server()
            exit()
        ml.remove_old_mods()
        ml.setup_loader()
        ml.force_resource_packs()
        ml.save_mod_info()
        if args.action == 'client':
            ml.start_client(args.account)
