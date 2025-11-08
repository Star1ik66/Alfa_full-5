import polars as pl
import os

# 🚀 Папка, где запущен скрипт
current_dir = os.getcwd()

# 🗂️ Укажи имена файлов, которые нужно объединить
files = [
    "mocsov_contacts.csv",
    "mocsov5_contacts.csv",
]

print(f"Текущая папка: {current_dir}")
print("Загружаем и объединяем файлы...")

dfs = []
for name in files:
    path = os.path.join(current_dir, name)
    if os.path.exists(path):
        try:
            # читаем как есть, игнорируем ошибки
            df = pl.read_csv(path, ignore_errors=True, infer_schema_length=10000)
            # 🧩 приводим все столбцы к строкам
            df = df.select([pl.col(c).cast(pl.Utf8).alias(c) for c in df.columns])
            dfs.append(df)
            print(f"  ✅ Файл загружен: {name} ({df.height} строк)")
        except Exception as e:
            print(f"  ⚠️ Ошибка при чтении {name}: {e}")
    else:
        print(f"  ⚠️ Файл не найден: {name}")

# 🔗 Склеиваем (одинаковые колонки — просто подряд)
if dfs:
    merged = pl.concat(dfs, how="vertical_relaxed")
    output_file = os.path.join(current_dir, "moscov_merged.csv")
    merged.write_csv(output_file)
    print(f"\n✅ Готово! Объединено {len(dfs)} файлов, итог: {merged.height} строк.")
    print(f"Сохранено в: {output_file}")
else:
    print("\n❌ Ошибка: ни один указанный файл не найден.")
