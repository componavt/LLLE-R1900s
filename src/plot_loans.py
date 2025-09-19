import os
import pandas as pd
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Загрузка конфигурации
load_dotenv('config.env')

# Чтение пути к выходному CSV-файлу из переменной окружения
csv_out_dir = os.getenv('CSV_OUT_DIR')
output_file_name = os.getenv('OUTPUT_CSV_FILE')  # Дополнительная переменная — см. ниже пояснение

# Если OUTPUT_CSV_FILE не задана, можно использовать жестко заданное имя или первое найденное
if not output_file_name:
    # Альтернатива: взять первый .csv файл в папке csv_out
    csv_files = [f for f in os.listdir(csv_out_dir) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("No CSV files found in the output directory.")
    output_file_name = csv_files[0]

csv_path = os.path.join(csv_out_dir, output_file_name)

# Загрузка данных
df = pd.read_csv(csv_path)

# Преобразование периода в дату: будем использовать середину периода (month_start и month_end)
# Для простоты возьмем средний месяц как (start + end) / 2, округленный вниз
df['mid_month'] = ((df['month_start'] + df['month_end']) // 2).astype(int)

# Создаем столбец с датой (первый день месяца)
df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['mid_month'].astype(str) + '-01')

# --- ГРАФИК 1: По поселениям (settlement) ---
grouped_by_settlement = df.groupby(['date', 'settlement'])['amount_rubles'].sum().reset_index()

plt.figure(figsize=(14, 7))
for settlement in grouped_by_settlement['settlement'].unique():
    subset = grouped_by_settlement[grouped_by_settlement['settlement'] == settlement]
    plt.plot(subset['date'], subset['amount_rubles'], label=settlement, marker='o', linestyle='-', linewidth=2)

plt.title('Loan Amounts Over Time by Settlement')
plt.xlabel('Date')
plt.ylabel('Total Loan Amount (Rubles)')
plt.legend(title='Settlement', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=3))
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('outputs/loans_by_settlement.png')
plt.show()

# --- ГРАФИК 2: По типам ссуд (credit_item) ---
grouped_by_credit = df.groupby(['date', 'credit_item'])['amount_rubles'].sum().reset_index()

plt.figure(figsize=(14, 7))
for credit_item in grouped_by_credit['credit_item'].unique():
    subset = grouped_by_credit[grouped_by_credit['credit_item'] == credit_item]
    plt.plot(subset['date'], subset['amount_rubles'], label=credit_item, marker='s', linestyle='--', linewidth=2)

plt.title('Loan Amounts Over Time by Credit Item')
plt.xlabel('Date')
plt.ylabel('Total Loan Amount (Rubles)')
plt.legend(title='Credit Item', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=3))
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('outputs/loans_by_credit_item.png')
plt.show()
