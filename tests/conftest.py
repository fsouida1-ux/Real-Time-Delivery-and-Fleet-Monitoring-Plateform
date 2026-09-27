"""
Configuration pytest partagée.

Rôle : rendre les modules des services importables depuis les tests,
sans avoir à transformer chaque service en package installable (ce qui
serait disproportionné pour du code applicatif qui ne sera jamais
importé ailleurs que par ses propres tests).

pytest charge ce fichier automatiquement avant de collecter les tests,
quel que soit le sous-dossier de tests/ concerné.
"""

import sys
from pathlib import Path

# tests/conftest.py -> .parent remonte à la racine du repo
# (le dossier qui contient tests/, services/, etc.)
REPO_ROOT = Path(__file__).parent.parent

# Chaque entrée correspond au dossier src/ d'un service dont on veut
# pouvoir importer les modules directement (ex: `from validation import ...`)
SERVICE_SRC_DIRS = [
    REPO_ROOT / "services" / "processing-consumer" / "src",
]

for src_dir in SERVICE_SRC_DIRS:
    sys.path.insert(0, str(src_dir))