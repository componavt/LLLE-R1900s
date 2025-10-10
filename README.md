# LLLE-R1900s
Loans, Livelihoods, and Local economies - Russia, 1900s

Кредиты, занятость и сельская экономика - Россия, начало XX века

# 🚀 Setup & Run in Cursor AI 
The program can be run as follows:
    
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -U pip
    pip install -r requirements.txt
    python src/python.py

# 📊 Visualizations & Notebooks
[src/visualization/grouped_bar_chart.ipynb](src/visualization/grouped_bar_chart.ipynb) compares the scale and frequency of loans for artisanal and migratory economic activities in early 20th-century rural Russia (сравнивает масштаб и частоту ссуд на ремесленные и сезонные заработки в сельской России начала XX века).  
[<img src="figures/grouped_bar_chart_Migration_CraftMaterials_CraftTools_ru.png" alt="Annual loan dynamics" width="600"/>](src/visualization/grouped_bar_chart.ipynb)

The same notebook also generates a schematic map with sparklines showing annual dynamics of loan amounts and counts for each credit society with two or more years of data (схематичная карта с мини-графиками (sparklines), показывающими динамику суммы и количества ссуд по годам для каждого товарищества с двумя и более годами данных).  
[<img src="figures/map_sparklines_count_ru.png" alt="Map with sparklines: loan count" width="600"/>](src/visualization/grouped_bar_chart.ipynb)

[src/visualization/percent_stacked_bar_by_credit.ipynb](src/visualization/percent_stacked_bar_by_credit.ipynb) generates percentage stacked bar charts showing the annual structure of loans by credit type as a percentage of the total volume (строит процентные столбчатые диаграммы, отражающие годовую структуру ссуд по типам кредитов в процентах от общего объёма).
