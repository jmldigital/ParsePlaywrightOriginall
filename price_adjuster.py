
# price_adjuster.py
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook
import pandas as pd
from config import corrected_price, competitor1, competitor2, competitor1_delivery, competitor2_delivery, input_price
from utils import logger
import re

def parse_delivery_days(delivery_str):
    """Парсит срок доставки 'X дней' в int или None"""
    if not delivery_str or not isinstance(delivery_str, str):
        return None
    m = re.search(r'(\d+)', delivery_str)
    if m:
        return int(m.group(1))
    return None



def adjust_prices_and_save(df, output_file):
    """
    Добавляет скорректированную цену и сохраняет DataFrame с цветовым форматированием.
    """
    corrected_prices = []

    for idx, row in df.iterrows():
        try:
            # Наша цена
            our_price = row[input_price]
            if pd.isna(our_price):
                corrected_prices.append(None)
                continue
            try:
                our_price = float(our_price)
            except (ValueError, TypeError):
                corrected_prices.append(None)
                continue

            # Цены конкурентов (только с доставкой 1–4 дня)
            competitor_prices = []

            # Проверяем competitor1
            comp1_price = row.get(competitor1)
            comp1_delivery = row.get(competitor1_delivery)
            if pd.notna(comp1_price) and pd.notna(comp1_delivery):
                try:
                    price_val = float(comp1_price)
                    delivery_days = parse_delivery_days(comp1_delivery)
                    if delivery_days is not None and 1 <= delivery_days <= 4:
                        competitor_prices.append(price_val)
                except:
                    pass

            # Проверяем competitor2
            comp2_price = row.get(competitor2)
            comp2_delivery = row.get(competitor2_delivery)
            if pd.notna(comp2_price) and pd.notna(comp2_delivery):
                try:
                    price_val = float(comp2_price)
                    delivery_days = parse_delivery_days(comp2_delivery)
                    if delivery_days is not None and 1 <= delivery_days <= 4:
                        competitor_prices.append(price_val)
                except:
                    pass

            # Логика корректировки
            if not competitor_prices:
                corrected_prices.append(our_price)
            else:
                min_comp_price = min(competitor_prices)
                if our_price > min_comp_price:
                    new_price = max(round(min_comp_price - 2, 2), 0.0)
                    corrected_prices.append(new_price)
                else:
                    corrected_prices.append(our_price)

        except Exception as e:
            logger.error(f"Ошибка при вычислении скорректированной цены для строки {idx}: {e}")
            corrected_prices.append(our_price)  # fallback

    # Добавляем колонку
    df[corrected_price] = corrected_prices

    # Сохраняем с форматированием
    try:
        df.to_excel(output_file, index=False, engine='openpyxl')
        wb = load_workbook(output_file)
        ws = wb.active

        # Находим индекс колонки с corrected_price
        corr_col_idx = None
        orig_col_idx = None

        for col in range(1, ws.max_column + 1):
            header = ws.cell(row=1, column=col).value
            if header == corrected_price:
                corr_col_idx = col
            if header == input_price:
                orig_col_idx = col

        if not corr_col_idx:
            logger.warning(f"Колонка '{corrected_price}' не найдена в заголовках")
            wb.save(output_file)
            return

        if not orig_col_idx:
            logger.warning(f"Колонка '{input_price}' не найдена в заголовках")
            wb.save(output_file)
            return

        # Цвета
        red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')  # снижена
        green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')  # без изменений

        # Применяем цвета к скорректированной цене
        for row_idx in range(2, ws.max_row + 1):
            try:
                orig_val = ws.cell(row=row_idx, column=orig_col_idx).value
                corr_val = ws.cell(row=row_idx, column=corr_col_idx).value

                if pd.isna(orig_val) or pd.isna(corr_val):
                    continue

                # Преобразуем в float
                try:
                    orig_val = float(orig_val)
                    corr_val = float(corr_val)
                except (ValueError, TypeError):
                    continue

                # Красный — если цена снижена
                if corr_val < orig_val:
                    ws.cell(row=row_idx, column=corr_col_idx).fill = red_fill
                else:
                    ws.cell(row=row_idx, column=corr_col_idx).fill = green_fill

            except Exception as cell_e:
                logger.debug(f"Ошибка форматирования строки {row_idx}: {cell_e}")
                continue

        wb.save(output_file)
        logger.info(f"✅ Файл сохранён с корректировкой цен и форматированием: {output_file}")

    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении Excel с форматированием: {e}")