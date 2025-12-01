import pkgutil
import importlib
import re

file_path = "bd_light_comercial.py"

# Regex para capturar todos os imports
pattern = r"^(?:import|from)\s+([a-zA-Z0-9_\.]+)"

imports = set()

with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        match = re.match(pattern, line.strip())
        if match:
            module = match.group(1).split(".")[0]
            imports.add(module)

print("📦 Bibliotecas detectadas no código:\n")
final_packages = []

for module in sorted(imports):
    try:
        pkg = importlib.import_module(module)
        if hasattr(pkg, "__file__") and "site-packages" in pkg.__file__:
            final_packages.append(module)
            print(f"→ {module}")
    except Exception:
        pass

print("\nCopie esses módulos para seu requirements.txt")

