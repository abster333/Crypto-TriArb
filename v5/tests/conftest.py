import sys
from pathlib import Path

# Ensure repository root is importable for `import v5.*`
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# Include legacy app/ modules (python-ingest/app) for reused components.
APP_ROOT = ROOT / "python-ingest"
if APP_ROOT.exists():
    if str(APP_ROOT) not in sys.path:
        sys.path.insert(0, str(APP_ROOT))
