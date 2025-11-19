# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_dynamic_libs
from PyInstaller.utils.hooks import collect_submodules
from importlib.util import find_spec

binaries = []
hiddenimports = ['agent_ohlc', 'mt5_client']
binaries += collect_dynamic_libs('MetaTrader5')
binaries += collect_dynamic_libs('numpy')
hiddenimports += collect_submodules('MetaTrader5')
hiddenimports += collect_submodules('numpy')

# --- ensure stdlib extension + requests' optional dep are bundled ---  #
hiddenimports += ['unicodedata', 'charset_normalizer','idna', 'urllib3']

spec_u = find_spec('unicodedata')
if spec_u and spec_u.origin and spec_u.origin.lower().endswith('.pyd'):
    binaries.append((spec_u.origin, '.'))


a = Analysis(
    ['xtl_installer.py'],
    pathex=[],
    binaries=binaries,
    datas=[],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['pandas', 'pandas.tests', 'numpy.tests'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='xtl',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='xtl',
)
