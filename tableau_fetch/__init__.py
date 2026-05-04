from pathlib import Path
from dotenv import load_dotenv

for _env_path in [Path.cwd() / ".env", Path(__file__).resolve().parent.parent / ".env"]:
    if _env_path.exists():
        load_dotenv(_env_path)
        break
