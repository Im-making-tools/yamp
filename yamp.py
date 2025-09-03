import asyncio
import dataclasses
import json
import re
import shutil
import sys
import time
import typing
import urllib
from collections import OrderedDict, namedtuple
from contextlib import ExitStack
import tomllib
import os
from datetime import datetime
from itertools import chain
from pathlib import Path
import logging
import subprocess
import lzma
from urllib.parse import urlparse
from zipfile import ZipFile, BadZipFile

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

JavaRuntime = namedtuple('JavaInstallation', ['ver', 'full_ver', 'runtime', 'bin'])

def get_installation(binary) -> JavaRuntime:
    if not os.access(binary, os.X_OK):
        raise ValueError(f"Binary {binary} is not executable")
    full_ver_str = subprocess.getoutput(f'{binary} -version').split('\n')
    if len(full_ver_str) < 3:
        raise ValueError(f"Binary {binary} returned invalid version: {full_ver_str}")
    java_ver_str, _, server_ver_str = full_ver_str[-3:]
    try:
        java_ver_str_split = java_ver_str.split(' ')
        ver_str = java_ver_str_split[2].strip('"')
        ver_parts = ver_str.split('_')[0].split('.')
        major_ver = int(ver_parts[1] if ver_parts[0] == '1' else ver_parts[0])
        runtime = 'openjdk'
        if 'GraalVM' in server_ver_str:
            runtime = 'graalvm'
        return JavaRuntime(major_ver, ver_str, runtime, binary)
    except (ValueError, IndexError):
        raise ValueError(f'Failed to parse java version: {java_ver_str} given by {binary}')

def installed_java_versions(search_path: str|Path = None, search_defaults=True) -> list[JavaRuntime]:
    java_path_candidates_unix = [
        '/usr/lib/jvm',
    ]
    java_path_candidates_nt = [
        'C:/Program Files (x86)/Java',
        'C:/Program Files/Java',
        'C:/Program Files (x86)/Oracle/Java',
        'C:/Program Files/Oracle/Java',
    ]
    java_path_candidates = []
    if search_defaults:
        java_path_candidates = java_path_candidates_nt if os.name == "nt" else java_path_candidates_unix
    if search_path is not None:
        java_path_candidates.append(search_path)
    java_bin = 'java.exe' if os.name == "nt" else 'java'
    versions = []
    for path_str in java_path_candidates:
        path = Path(path_str)
        for binary in chain(path.glob(f'*/jre/bin/{java_bin}'), path.glob(f'*/bin/{java_bin}')):
            java_root = path / binary.relative_to(path).parts[0]
            if java_root.is_symlink() and  java_root.resolve().is_relative_to(path):
                continue  # ignore symlinks that point back to other java versions
            try:
                java = get_installation(binary)
                versions.append(java)
            except ValueError as e:
                console.print(f"[bright_red]ERROR[/bright_red] {e.args[0]}")
    return versions

def test_symlink():
    """ Symbolic links are not a thing by default only on windows """
    if os.name != 'nt':
        return
    import tempfile
    tmp_dir = Path(tempfile.gettempdir())
    symlink_dest = tmp_dir / '.yamp_symlink_test'
    try:
        os.symlink(tmp_dir, symlink_dest)
    except OSError as e:
        if e.winerror == 1314:  # No permissions. Win10/11 needs developer mode.
            print("[bold]YAMP[/bold] uses symbolic links to save disk space. "
                             "This feature is not enabled by default on windows."
                             "To enable, please turn [green]on[/green] [yellow]Developer Mode")
            subprocess.run(['powershell', '-command', 'Start-Process "ms-settings:developers"'])
            exit(1)
        else:
            raise e
    finally:
        symlink_dest.unlink(missing_ok=True)


url_to_name = {}
class DownloadQueue:
    chunk_size = 1024 * 64
    def __init__(self, cache_url=None):
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
                filename.parent.mkdir(parents=True, exist_ok=True)
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

def get_content_file(h):
    return re.findall("filename=\"?([^\"]+)\"?", h.headers['content-disposition'])[0]

def download_override(self):
    q = DownloadQueue()
    q.q = self.q
    q.size = self.size
    return q.download()

def file_with_dir(path: Path) -> str:
    return f'{path.parent.name}/{path.name}'

OriginalDownloadQueue.download = download_override


def update_symlink(src_file: Path, dest_file: Path):
    if dest_file.exists():
        if not dest_file.is_symlink():
            MainLauncher.log.warning(f"File is not symlink? [yellow]{dest_file.name}")
            return False
        if dest_file.readlink() == src_file:
            return False
        dest_file.unlink()
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(src_file, dest_file)
    return True

def get_manifest_override(self):
    from picomc.logging import logger

    manifest_filepath: Path = self.launcher.get_path(Directory.VERSIONS, "manifest.json")
    if manifest_filepath.exists():
        try:
            m = requests.get(self.MANIFEST_URL, timeout=1).json()  # Sometimes server takes forever to respond!
            manifest_filepath.write_text(json.dumps(m, indent=4, sort_keys=True))
            return m
        except (requests.ConnectionError, requests.exceptions.ReadTimeout):
            logger.warning("Failed to retrieve version_manifest.")
            return json.loads(manifest_filepath.read_text())
    else:
        try:
            m = requests.get(self.MANIFEST_URL, timeout=MainLauncher.TIMEOUT).json()
            manifest_filepath.write_text(json.dumps(m, indent=4, sort_keys=True))
            return m
        except (requests.ConnectionError, requests.exceptions.ReadTimeout):
            logger.warning(
                "Failed to retrieve version_manifest and no local version available."
                "Check your internet connection."
            )
            raise RuntimeError(f"Failed to retrieve version manifest {self.MANIFEST_URL}")


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

def find(root: dict, key: str, default=None):
    keys = key.split('.')
    d = root
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k, None)
    if d is None:
        return default
    return d


Resource = typing.NamedTuple("Resource", [('name', str), ('source', str), ('type', str), ('options', dict)])


class MainLauncher:
    TIMEOUT = 20
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
            self.RESOURCES[f'{source}-{modid}'] = Resource(modid, source, typ, options)


    def __init__(self, es, config_file: str, root_dir: str|None = None, java: str|None = JAVA, debug=False):
        self.session = requests.Session()
        self.debug = debug
        if not root_dir:
            if os.name == 'nt':
                root_dir = os.environ.get("YAMP_ROOT", "C:/Games/Minecraft")
            else:
                root_dir = os.environ.get("YAMP_ROOT", "~/Games/Minecraft")
        self.MINECRAFT_DIR = Path(root_dir).expanduser()
        self.DOWNLOAD_DIR = self.MINECRAFT_DIR / 'downloads'
        self.JAVA_DIR = self.MINECRAFT_DIR / 'java'

        if config_file.startswith('http'):
            res = self.session.get(config_file)
            res.raise_for_status()
            config_data = res.text
        else:
            config_data = Path(config_file).read_text()
        config = tomllib.loads(config_data)
        self.MC_VERSION: str = config['minecraft']['version']
        self.LOADER: str = config['minecraft']['loader'].lower()
        self.LOADER_VER: str|None = config['minecraft'].get('loader_ver')
        self.VERSION: str = f"{self.MC_VERSION}-{self.LOADER}-{self.LOADER_VER}"
        self.PACK_NAME: str = config['minecraft']['pack_name']
        self.RESOURCES: dict[str, Resource] = {}
        self.add_resources(config.get('mods', {}), 'mods')
        self.add_resources(config.get('resourcepacks', {}), 'resourcepacks')
        self.add_resources(config.get('shaders', {}), 'shaderpacks')
        self.add_resources(config.get('datapacks', {}), 'datapacks')

        self.SOURCE_MAP = {
            'curseforge': self._fetch_curseforge_resource,
            'modrinth': self._fetch_modrinth_resource,
            'url': self._fetch_url_resource,
            'file': self._fetch_url_resource,
        }

        self.es = es
        self.launcher = Launcher(self.es, root=self.MINECRAFT_DIR / "picomc")
        self.am = self.launcher.account_manager
        self.config = config
        self.inst = self.get_instance(java)
        self.cached = {}
        self.db = {'_loaded': False}
        self.old_files = set()
        self.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
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
                    self.LOADER_VER,
                    version_name=self.VERSION,
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
        im = self.launcher.instance_manager
        if not im.exists(self.PACK_NAME):
            inst = im.create(self.PACK_NAME, self.VERSION)
        else:
            inst = im.get(self.PACK_NAME)
        if 'java_max_memory' in self.config['minecraft']:
            inst.config['java.memory.max'] = self.config['minecraft']['java_max_memory']
        if java:
            inst.config['java.path'] = str(java)
        elif 'java.path' not in inst.config or not Path(inst.config['java.path']).exists():
            version = self.launcher.version_manager.get_version(self.MC_VERSION)
            java_ver = version.java_version.get('majorVersion', 8)
            local_java_path = self.JAVA_DIR / f'openjdk_{java_ver}'
            java_vers = {v.ver: v for v in installed_java_versions(local_java_path)}
            if java_ver not in java_vers:
                import jdk
                dq = DownloadQueue()
                jdk_url = jdk.get_download_url(version=str(java_ver))
                h = self.session.head(jdk_url, allow_redirects=True)
                jdk_file = self.DOWNLOAD_DIR / get_content_file(h)
                local_java_path.mkdir(parents=True, exist_ok=True)
                dq.add(h.url, jdk_file, int(h.headers.get('content-length', 0)))
                self.log.info(f"Downloading [blue]java {java_ver}[/blue].. [gray50]{jdk_file}")
                dq.download()
                self.log.info(f"Decompressing [blue]java {java_ver}[/blue]..")
                jdk_ext = jdk.extractor.get_compressed_file_ext(str(jdk_file))
                jdk._decompress_archive(str(jdk_file), jdk_ext, str(local_java_path))
                java_vers = {v.ver: v for v in installed_java_versions(local_java_path)}
            inst.config['java.path'] = str(java_vers[java_ver].bin)
            self.log.info(f"Using [blue]java {java_ver}[/blue] from [yellow]{java_vers[java_ver].bin}")
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
            if len(files) == 0:  # try without loader
                files = sorted(filter(lambda x: self.MC_VERSION in x['versions'], data['response']['files']), key=lambda x: -x['id'])
            if len(files) == 0:
                versions = set(v for f in data['response']['files'] for v in f['versions'])
                raise ValueError(f"No versions available for {find(data, 'response.title', '<no title>')} "
                                 f"{modid} ({find(data, 'response.url.curseforge', '<no url>')}, files: {len(data['response']['files'])} versions: {versions}")
            file = files[0]
        sid = str(file['id'])
        data['latest_file'] = {
            'filename': file['name'],
            'url': f'https://mediafilez.forgecdn.net/files/{sid[:-3].lstrip('0')}/{sid[-3:].lstrip('0')}/{file['name']}',
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
            res = self.session.get(f"https://api.modrinth.com/v2/project/{proj_id}", timeout=self.TIMEOUT)
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
        elif typ == "datapack":
            params['loaders'] = json.dumps([options.get('loader', 'datapack')])
        res = self.session.get(f'https://api.modrinth.com/v2/project/{modid}/version', params=params, timeout=self.TIMEOUT)
        res.raise_for_status()
        res_data = res.json()
        if len(res_data) == 0:
            res2 = self.session.get(f'https://api.modrinth.com/v2/project/{modid}', params=params, timeout=self.TIMEOUT).json()
            raise ValueError(f"No versions available for {res2['title']} {modid} https://modrinth.com/mod/{res2['slug']}, available: {', '.join(res2['game_versions'])} for [cyan]{'[/cyan], [cyan]'.join(res2['loaders'])}[/cyan]")
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
                            'url': f'https://mediafilez.forgecdn.net/files/{sid[:-3].lstrip('0')}/{sid[-3:].lstrip('0')}/{files[0]['name']}',
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
            data['disallow'] = opt.get('disallow', False) or opt.get('debug', False) and not self.debug
            filename_output: Path = self.inst.directory / 'minecraft' / data['type'] / data['latest_file']['filename']
            filename: Path = self.DOWNLOAD_DIR / data['latest_file']['hash'] / data['latest_file']['filename']
            cache[data['rid']] = data
            self.mapping[rid] = data['rid']
            if not data['disallow']:
                url_to_name[data['latest_file']['url']] = data['rid'], data['latest_file']['filename']
                downloads.append((data['latest_file']['url'], filename_output, data['latest_file']['size'], data['latest_file']['hash']))
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

    def setup_files(self, update=False, server=False):
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
            resources = self.RESOURCES
            if server:
                resources = {k: v for k, v in resources.items() if v.type in {'mods', 'datapacks'}}
            self.cached, downloads, error = self.fetch_resources(resources, {}, prog, check_update=update)
        save_json_xz(modlist_file, {'_mod_dict': self.cached, '_mapping': self.mapping})
        if error:
            raise Exception("Stopping because of previous errors")
        dq = DownloadQueue()
        # Removes duplicates
        downloads = {hash: (url, fname, size) for url, fname, size, hash in downloads}
        for hash, (url, fname, size) in downloads.items():
            if (self.DOWNLOAD_DIR / hash / fname.name).exists() or url.startswith('file://'):
                continue
            (self.DOWNLOAD_DIR / hash).mkdir(exist_ok=True)
            dq.add(url, self.DOWNLOAD_DIR / hash / fname.name, size)
        if len(dq) > 0:
            self.log.info("Downloading [blue]mods[/blue]..")
            dq.download()
        for hash, (url, fname, size) in downloads.items():
            fname.parent.mkdir(parents=True, exist_ok=True)
            src_file = self.DOWNLOAD_DIR / hash / fname.name
            if url.startswith('file://'):
                src_file = Path(url[7:])
            if update_symlink(src_file, fname):
                self.log.info(f"Updated file [yellow]{file_with_dir(fname)}")
            if fname.name in self.old_files:
                self.old_files.remove(fname.name)
        # for mod in self.cached.values():
        #     if not mod['used'] or mod['type'] != 'mods' or mod['disallow']:
        #         continue
        #     if mod['latest_file']['filename'] in self.old_files:
        #         self.old_files.remove(mod['latest_file']['filename'])
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

    def check_zip_files(self):
        zip_files = list((self.inst.directory / 'minecraft' / 'mods').glob('*.jar'))
        zip_files += list((self.inst.directory / 'minecraft' / 'resourcepacks').glob('*.zip'))
        zip_files += list((self.inst.directory / 'minecraft' / 'shaderpacks').glob('*.zip'))
        zip_files = sorted(zip_files)
        self.log.info("Checking for corrupt zip files..")
        with progress.Progress(
            progress.TextColumn("[progress.description]{task.description}"),
            progress.DownloadColumn(binary_units=True),
            progress.TransferSpeedColumn(),
            progress.TimeRemainingColumn(),
            progress.TextColumn("[gray50]{task.fields[id]}"),
            console=console
        ) as prog:
            task = prog.add_task('Checking zip files..', total=0, start=False, id='')
            total = 0
            chunk_size = 2 ** 20
            bad: set[Path] = set()
            for zip in zip_files:
                try:
                    zip_file = ZipFile(zip)
                    for zinfo in zip_file.filelist:
                        total += zinfo.file_size
                        prog.update(task, total=total, id=zip.name)
                except BadZipFile:
                    bad.add(zip)
                    self.log.error(f"Corrupt zip file: [yellow]{zip}[/yellow]")
            prog.start_task(task)
            prog.update(task, description="Reading zip files..")
            for zip in zip_files:
                if zip in bad:
                    continue
                zip_file = ZipFile(zip)
                for zinfo in zip_file.filelist:
                    prog.update(task, id=zip.name)
                    try:
                        with zip_file.open(zinfo.filename, "r") as f:
                            while True:  # Check CRC-32
                                d = f.read(chunk_size)
                                if not d:
                                    break
                                prog.advance(task, len(d))
                    except BadZipFile:
                        bad.add(zip)
                        self.log.error(f"Corrupt zip file: [yellow]{zip}[/yellow]")
            for zip_file in bad:
                zip_file = zip_file.resolve()
                self.log.info(f"Deleting: [yellow]{zip_file}[/yellow]")
                zip_file.unlink()

    def write_pack_info(self):
        existing = {m['latest_file']: m for m in self.cached.values()}

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
            elif 'update' in options:
                if filename.lower().endswith('.json'):
                    loader = json.loads
                    dumper = lambda x: json.dumps(x, indent=4)
                elif filename.lower().endswith('.json5'):
                    import json5
                    loader = json5.loads
                    dumper = lambda x: json5.dumps(x, indent=4)
                elif filename.lower().endswith('.toml'):
                    import tomli_w
                    loader = tomllib.loads
                    dumper = tomli_w.dumps
                elif filename.lower().endswith('.yaml'):
                    from ruamel.yaml import YAML
                    import io
                    yaml = YAML()
                    dump_stream = io.StringIO()
                    loader = yaml.load
                    def dumper(obj):
                        yaml.dump(obj, stream=dump_stream)
                        output_str = dump_stream.getvalue()
                        dump_stream.close()
                        return output_str
                else:
                    # TODO: add jsonc, cfg
                    raise ValueError(f"Unsupported file extension: {filename}")
                if filepath.exists():
                    exising_data = loader(filepath.read_text())
                    if isinstance(exising_data, dict):
                        exising_data.update(options['update'])
                    else:
                        raise ValueError(f"Unsupported file content type {type(exising_data)}: {filename}")
                else:
                    exising_data = options['update']
                file_data = dumper(exising_data)
            elif 'replace' in options:
                if filepath.exists():
                    file_data = filepath.read_text()
                    for key, value in options['replace'].items():
                        file_data = file_data.replace(key, value)
            if file_data is not None:
                current_hash = xxhash.xxh64(filepath.read_bytes().strip() if filepath.exists() else b'').digest()
                update_hash = xxhash.xxh64(file_data.encode().strip()).digest()
                if current_hash != update_hash:
                    self.log.info(f"Updating file [yellow]{filename}")
                    os.makedirs(filepath.parent, exist_ok=True)
                    filepath.write_text(file_data)

    def write_options(self):
        resource = {k: self.cached[self.mapping[k]] for k in self.RESOURCES.keys() if self.RESOURCES[k].type == 'resourcepacks'}
        if len(resource) == 0:
            return

        options_file: Path = self.inst.directory / 'minecraft' / 'options.txt'
        if not options_file.exists():
            packs = ['file/' + r['latest_file']['filename'] for r in resource.values()]
            options_file.write_text(f'resourcePacks:{json.dumps(packs)}')
            return
        options = OrderedDict()
        for line in options_file.read_text().splitlines():
            key = line.split(':')[0]
            value = ':'.join(line.split(':')[1:])
            options[key] = value

        packs = json.loads(options.get('resourcePacks', '[]'))
        packs2 = json.loads(options.get('incompatibleResourcePacks', '[]'))
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
        options.update(self.config.get('client', {}).get('options', {}))
        self.log.info(f"Updating option [yellow]{options['resourcePacks']}")
        options_file.write_text('\n'.join([f'{k}:{v}' for k, v in options.items()]))
        #     resourcePacks:["vanilla","mod_resources","Moonlight Mods Dynamic Assets","firmalife_data","file/Serified Font v1.1 f1-34.zip"]

    def start_server(self, directory: Path | str | None = None):
        server_dir: Path = self.inst.directory / 'server' if directory is None else Path(directory)
        server_dir.mkdir(exist_ok=True, parents=True)
        self.log.info(f"Server directory: [yellow]{server_dir}")
        if not (server_dir / 'libraries').exists():
            os.symlink(self.launcher.root / 'libraries', server_dir / 'libraries')
        fabric_jar_file = self.DOWNLOAD_DIR / 'installers' / f'fabric_server_{self.MC_VERSION}.jar'
        if self.LOADER == 'forge' and not (server_dir / 'run.sh').exists():
            ver = f'{self.MC_VERSION}-{self.LOADER_VER}'
            h = self.session.head(f'https://maven.minecraftforge.net/net/minecraftforge/forge/{ver}/forge-{ver}-installer.jar')
            h.raise_for_status()
            installer_file = self.DOWNLOAD_DIR / 'installers' / get_content_file(h)
            dq = DownloadQueue()
            dq.add(h.url, installer_file, int(h.headers.get('content-length', 0)))
            self.log.info(f"Downloading [blue]server installer[/blue].. [gray50]{installer_file}")
            dq.download()
            self.log.info("Running [blue]server installer[/blue]..")
            subprocess.run([self.inst.get_java(), '-jar', str(installer_file), '--installServer'], cwd=server_dir)
        elif self.LOADER == 'fabric' and not fabric_jar_file.exists():
            res = self.session.get('https://mcutils.com/api/server-jars/fabric')
            res.raise_for_status()
            fabric_ver = {i['version']: i['url'] for i in res.json()}
            if self.MC_VERSION not in fabric_ver:
                raise Exception(f"No Fabric version found for {self.MC_VERSION}")
            res = self.session.get(fabric_ver[self.MC_VERSION])
            res.raise_for_status()
            dq = DownloadQueue()
            dq.add(res.json()['downloadUrl'], fabric_jar_file, 0)
            self.log.info(f"Downloading [blue]server file[/blue].. [gray50]{fabric_jar_file}")
            dq.download()
        elif self.LOADER not in {'forge', 'fabric'}:
            raise ValueError(f'Unsupported loader: {self.LOADER}')

        if not (server_dir / 'eula.txt').exists():
            console.input("Press [bold cyan]enter[/bold cyan] to accept the Minecraft EULA..")
            (server_dir / 'eula.txt').write_text('eula=TRUE\n')
        if not (server_dir / 'user_jvm_args.txt').exists():
            (server_dir / 'user_jvm_args.txt').write_text('# Uncomment the next line to set it.\n# -Xmx4G\n')
        if not (server_dir / 'server.properties').exists():
            fields = [f'{key}={value}' for key, value in self.config.get('server', {}).items()]
            (server_dir / 'server.properties').write_text('\n'.join(fields))
        (server_dir / 'mods').mkdir(exist_ok=True)
        (server_dir / 'datapacks').mkdir(exist_ok=True)
        old_files = set((server_dir / 'mods').glob('*.jar')) | set((server_dir / 'datapacks').glob('*.zip'))
        added = set()
        for mod in self.cached.values():
            if not mod['used'] or mod['type'] not in {'mods', 'datapacks'} or mod['disallow'] or mod['options'].get('client_only', False):
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
            dest_file: Path = server_dir / mod['type'] / fname
            src_file = self.DOWNLOAD_DIR / hash / fname
            self.log.info(f"Adding mod: [bright_blue]{trunc_title:30}\t[yellow]{fname}[/yellow]")
            if url.startswith('file://'):
                src_file = Path(url[7:])
            update_symlink(src_file, dest_file)
            if dest_file in old_files:
                old_files.remove(dest_file)
        self.save_mod_info()
        for old_file in old_files:
            self.log.warning(f"Removing mod file [yellow]{file_with_dir(old_file)}")
            old_file.unlink(missing_ok=True)
        self.update_custom_files(server_dir)
        srv_args = [self.inst.get_java()]
        if 'java_max_memory' in self.config['minecraft']:
            srv_args += [f"-Xmx{self.config['minecraft']['java_max_memory']}"]
        if self.LOADER == 'forge':
            lib_file = 'win_args.txt' if os.name == 'nt' else 'unix_args.txt'
            lib_path = f'libraries/net/minecraftforge/forge/{self.MC_VERSION}-{self.LOADER_VER}/{lib_file}'
            jar_file = server_dir / f'libraries/net/minecraftforge/forge/{self.MC_VERSION}-{self.LOADER_VER}/forge-{self.MC_VERSION}-{self.LOADER_VER}.jar'
            if (server_dir / lib_path).exists():
                srv_args += ['@user_jvm_args.txt', '@' + lib_path]
            else:
                srv_args += ['@user_jvm_args.txt', '-jar', str(jar_file)]
        if self.LOADER == 'fabric':
            srv_args += ['@user_jvm_args.txt', '-jar', str(fabric_jar_file)]
        srv_args += ['-nogui']
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

    def start_client(self, account_name=None, singleplayer=None, multiplayer=None, no_prime=False):
        self.log.info(f"Launching modpack [yellow]{self.PACK_NAME}")
        self.log.info(f"Client directory: [yellow]{self.inst.directory / 'minecraft'}")
        self.inst.features = {
            'is_quick_play_singleplayer': singleplayer is not None,
            'is_quick_play_multiplayer': multiplayer is not None,
        }
        self.inst.features_args = {
            'quickPlaySingleplayer': singleplayer or '',
            'quickPlayMultiplayer': multiplayer or '',
        }
        account = self.get_account(account_name)
        if os.name == 'posix' and not no_prime:
            output = subprocess.getoutput(['lspci', '-nn'])
            if "nvidia" in output.lower():
                os.environ.setdefault('__NV_PRIME_RENDER_OFFLOAD', '1')
                os.environ.setdefault('__VK_LAYER_NV_optimus', 'NVIDIA_only')
                os.environ.setdefault('__GLX_VENDOR_LIBRARY_NAME', 'nvidia')
        self.inst.launch(account)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        prog='YAMP', description='Yet Another Minecraft Packmaker. Download and run Minecraft modpacks')
    parser.add_argument('-u', '--update', action='store_true', help='Check for updates and download latest')
    parser.add_argument('-a', '--account', default=None, help='Minecraft username to use')
    parser.add_argument('-D', '--debug', action='store_true', help='Enable debug environment')
    parser.add_argument('--no-prime', action='store_true', help='Disable prime rendering (linux with nvidia only, when disabled run on integrated gpu)')
    parser.add_argument('--no-greet', action='store_true', help='Do not print geeter')
    parser.add_argument('--java', default=None, help='Specify java path')
    parser.add_argument('--singleplayer', default=None, help='Open singleplayer world')
    parser.add_argument('--multiplayer', default=None, help='Open multiplayer server')
    parser.add_argument('--timeout', default=30, type=int, help='Network connection timeout for downloading resources')
    parser.add_argument('pack_file', help='Modpack toml filename or url')
    parser.add_argument('action', choices=['client', 'java', 'check', 'server', 'loader_vers', 'check_zip'], help='Specify action to do')

    args = parser.parse_args()
    test_symlink()
    if not args.no_greet:
        # Generated with https://pypi.org/project/art/
        console.print(r"""[bright_blue]                  ___           ___           ___   
      ___        /  /\         /__/\         /  /\  
     /__/|      /  /::\       |  |::\       /  /::\ 
    |  |:|     /  /:/\:\      |  |:|:\     /  /:/\:\
    |  |:|    /  /:/~/::\   __|__|:|\:\   /  /:/~/:/
  __|__|:|   /__/:/ /:/\:\ /__/::::| \:\ /__/:/ /:/ 
 [bright_yellow]/__/::::\   \  \:\/:/__\/ \  \:\~~\__\/ \  \:\/:/  
    ~\~~\:\   \  \::/       \  \:\        \  \::/   
      \  \:\   \  \:\        \  \:\        \  \:\   
       \__\/    \  \:\        \  \:\        \  \:\  
                 \__\/         \__\/         \__\/                    
       [italic gray50]Yet Another Minecraft Packmaker.
        """)
    import picomc.downloader
    import mock
    with mock.patch.object(picomc.downloader, 'DownloadQueue', DownloadQueue), ExitStack() as es:
        MainLauncher.TIMEOUT = args.timeout
        ml = MainLauncher(es, args.pack_file, java=args.java, debug=args.debug)
        ml.log.setLevel(logging.INFO)
        if args.action == 'loader_vers':
            ml.list_loader_versions()
            exit()
        ml.setup_files(args.update, server=args.action == 'server')
        if args.action == 'check_zip':
            ml.check_zip_files()
            exit()
        if args.action == 'check':
            ml.list_mods_files()
            ml.save_mod_info()
        if args.action == 'server':
            ml.start_server()
            exit()
        ml.remove_old_mods()
        ml.setup_loader()
        ml.write_options()
        ml.save_mod_info()
        if args.action == 'client':
            ml.start_client(args.account, args.singleplayer, args.multiplayer, args.no_prime)


if __name__ == "__main__":
    main()