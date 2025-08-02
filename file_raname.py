import os

folder = r"D:\vscode\KTK\Najuskalo\backend\json"

for fname in os.listdir(folder):
    if "_20250801_" in fname:
        new_fname = fname.replace("_20250801_", "_20250802_")
        src = os.path.join(folder, fname)
        dst = os.path.join(folder, new_fname)
        if src != dst:
            print(f"Renaming: {fname} -> {new_fname}")
            os.rename(src, dst)