# price_adjuster.py
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook
import pandas as pd
from config import corrected_price, competitor1, competitor2, competitor1_delivery, competitor2_delivery, input_price,INPUT_COL_ARTICLE,INPUT_COL_BRAND       
        
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
    Подробно логирует процесс обработки строк и вычислений.
    """

    corrected_prices = []

    def parse_price(value):
        """Безопасно преобразует цену в float (заменяя запятую на точку, убирая пробелы)."""
        if pd.isna(value):
            return None
        try:
            val_str = str(value).strip().replace(" ", "").replace(",", ".")
            return float(val_str)
        except Exception:
            return None

    for idx, row in df.iterrows():
        # logger.info("------------------------------------------------------------")
        # logger.info(f"▶️ Обработка строки {idx + 1}/{len(df)}")

        try:
            # Наша цена
            raw_price = row.get(input_price)
            

            our_price = parse_price(raw_price)
            if our_price is None:
                # logger.warning(f"⚠️ Невозможно преобразовать цену: {raw_price}")
                corrected_prices.append(None)
                continue

            # logger.info(f"✅ our_price (float): {our_price}")

            # Цены конкурентов (только с доставкой 1–4 дня)
            competitor_prices = []

            # Проверяем competitor1
            comp1_price = parse_price(row.get(competitor1))
            comp1_delivery = row.get(competitor1_delivery)
            # logger.info(f"competitor1_price: {comp1_price}, delivery: {comp1_delivery}")

            if comp1_price is not None and pd.notna(comp1_delivery):
                delivery_days = parse_delivery_days(comp1_delivery)
                if delivery_days is not None and 1 <= delivery_days <= 4:
                    competitor_prices.append(comp1_price)
                    # logger.info(f"✅ competitor1 добавлен ({comp1_price} за {delivery_days} дн.)")

            # Проверяем competitor2
            comp2_price = parse_price(row.get(competitor2))
            comp2_delivery = row.get(competitor2_delivery)
            # logger.info(f"competitor2_price: {comp2_price}, delivery: {comp2_delivery}")

            if comp2_price is not None and pd.notna(comp2_delivery):
                delivery_days = parse_delivery_days(comp2_delivery)
                if delivery_days is not None and 1 <= delivery_days <= 4:
                    competitor_prices.append(comp2_price)
                    # logger.info(f"✅ competitor2 добавлен ({comp2_price} за {delivery_days} дн.)")

            # Логика корректировки
            if not competitor_prices:
                corrected_prices.append(our_price)
                # logger.info("ℹ️ Нет подходящих конкурентов, цена без изменений.")
            else:
                min_comp_price = min(competitor_prices)
                if our_price > min_comp_price:
                    new_price = max(round(min_comp_price - 2, 2), 0.0)
                    corrected_prices.append(new_price)
                    # logger.info(f"🔻 Цена снижена: {our_price} → {new_price}")
                else:
                    corrected_prices.append(our_price)
                    # logger.info(f"✅ Цена конкурентоспособна: {our_price}")

        except Exception as e:
            logger.error(f"Ошибка при вычислении скорректированной цены для строки {idx}: {e}")
            corrected_prices.append(None)

    # Добавляем колонку
    df[corrected_price] = corrected_prices

    # Сохраняем с форматированием
    try:
        df.to_excel(output_file, index=False, engine='openpyxl')
        wb = load_workbook(output_file)
        ws = wb.active

        corr_col_idx = None
        orig_col_idx = None

        for col in range(1, ws.max_column + 1):
            header = ws.cell(row=1, column=col).value
            if header == corrected_price:
                corr_col_idx = col
            if header == input_price:
                orig_col_idx = col

        if not corr_col_idx or not orig_col_idx:
            logger.warning("⚠️ Не удалось определить индексы колонок для подсветки")
            wb.save(output_file)
            return

        red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')  # снижена
        green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')  # без изменений

        for row_idx in range(2, ws.max_row + 1):
            try:
                orig_val = ws.cell(row=row_idx, column=orig_col_idx).value
                corr_val = ws.cell(row=row_idx, column=corr_col_idx).value

                if orig_val is None or corr_val is None:
                    continue

                orig_val = parse_price(orig_val)
                corr_val = parse_price(corr_val)

                if orig_val is None or corr_val is None:
                    continue

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





# def parse_delivery_days(delivery_str):
#     """Парсит срок доставки 'X дней' в int или None"""
#     if not delivery_str or not isinstance(delivery_str, str):
#         return None
#     m = re.search(r'(\d+)', delivery_str)
#     if m:
#         return int(m.group(1))
#     return None


# def adjust_prices_and_save(df, output_file):
#     """
#     Добавляет скорректированную цену и сохраняет DataFrame с цветовым форматированием.
#     С подробным логированием для отладки.
#     """
#     logger.info("=" * 60)
#     logger.info("🔧 НАЧАЛО КОРРЕКТИРОВКИ ЦЕН")
#     logger.info(f"Всего строк: {len(df)}")
#     logger.info(f"Доступные столбцы: {list(df.columns)}")

#     # Проверка, что нужные столбцы существуют
#     missing_cols = []
#     for col in [input_price, competitor1, competitor2, competitor1_delivery, competitor2_delivery]:
#         if col not in df.columns:
#             missing_cols.append(col)
#             logger.error(f"❌ Столбец '{col}' отсутствует в DataFrame!")

#     if missing_cols:
#         logger.error(f"Прерываю корректировку: отсутствуют столбцы: {missing_cols}")
#         # Всё равно сохраняем, но corrected_price = None
#         df[corrected_price] = None
#         df.to_excel(output_file, index=False, engine='openpyxl')
#         return df

#     logger.info(f"✅ Все необходимые столбцы найдены")
#     logger.info(f"📌 Используем входную цену: '{input_price}'")
#     logger.info(f"📌 Конкурент 1: {competitor1} + {competitor1_delivery}")
#     logger.info(f"📌 Конкурент 2: {competitor2} + {competitor2_delivery}")

#     corrected_prices = []
#     any_competitor_found = False  # флаг: были ли конкуренты в диапазоне 1–4 дня

#     # Обработка каждой строки
#     for idx, row in df.iterrows():
#         try:
#             # === Идентификация строки ===
#             brand = row.get('Бренд', row.get('brand', row.get(INPUT_COL_BRAND, 'Unknown')))
#             article = row.get('Артикул', row.get('article', row.get(INPUT_COL_ARTICLE, 'Unknown')))
#             logger.debug(f"--- Обработка строки {idx}: {brand} / {article}")

#             # === 1. Наша цена ===
#             our_price_raw = row[INPUT_COL_PRICE]
#             logger.debug(f"  → Наша цена (сырая): {our_price_raw}")

#             if pd.isna(our_price_raw):
#                 logger.debug(f"  ⚠️ Наша цена — NaN → corrected_price = None")
#                 corrected_prices.append(None)
#                 continue

#             try:
#                 our_price = float(our_price_raw)
#                 logger.debug(f"  → Наша цена (число): {our_price:.2f} ₽")
#             except (ValueError, TypeError) as e:
#                 logger.debug(f"  ❌ Не удалось преобразовать '{our_price_raw}' в число: {e}")
#                 corrected_prices.append(None)
#                 continue

#             # === 2. Цены конкурентов (только с доставкой 1–4 дня) ===
#             competitor_prices = []

#             # Проверяем competitor1
#             comp1_price_raw = row.get(competitor1)
#             comp1_delivery_raw = row.get(competitor1_delivery)
#             logger.debug(f"  → {competitor1}: цена={comp1_price_raw}, доставка={comp1_delivery_raw}")

#             if pd.notna(comp1_price_raw) and pd.notna(comp1_delivery_raw):
#                 try:
#                     comp1_price = float(comp1_price_raw)
#                     comp1_days = parse_delivery_days(comp1_delivery_raw)
#                     logger.debug(f"  → {competitor1} — распаршенные дни: {comp1_days}")

#                     if comp1_days is not None and 1 <= comp1_days <= 4:
#                         competitor_prices.append(comp1_price)
#                         logger.debug(f"  ✅ {competitor1} добавлен: {comp1_price:.2f} ₽ (доставка {comp1_days} дн)")
#                         any_competitor_found = True
#                     else:
#                         logger.debug(f"  ⚠️ {competitor1} — доставка вне 1–4 дней → игнорируем")
#                 except Exception as e:
#                     logger.debug(f"  ❌ Ошибка при обработке {competitor1}: {e}")

#             # Проверяем competitor2
#             comp2_price_raw = row.get(competitor2)
#             comp2_delivery_raw = row.get(competitor2_delivery)
#             logger.debug(f"  → {competitor2}: цена={comp2_price_raw}, доставка={comp2_delivery_raw}")

#             if pd.notna(comp2_price_raw) and pd.notna(comp2_delivery_raw):
#                 try:
#                     comp2_price = float(comp2_price_raw)
#                     comp2_days = parse_delivery_days(comp2_delivery_raw)
#                     logger.debug(f"  → {competitor2} — распаршены дни: {comp2_days}")

#                     if comp2_days is not None and 1 <= comp2_days <= 4:
#                         competitor_prices.append(comp2_price)
#                         logger.debug(f"  ✅ {competitor2} добавлен: {comp2_price:.2f} ₽ (доставка {comp2_days} дн)")
#                         any_competitor_found = True
#                     else:
#                         logger.debug(f"  ⚠️ {competitor2} — доставка вне 1–4 дней → игнорируем")
#                 except Exception as e:
#                     logger.debug(f"  ❌ Ошибка при обработке {competitor2}: {e}")

#             # === 3. Логика корректировки ===
#             logger.debug(f"  → Цены конкурентов (1–4 дня): {competitor_prices}")

#             if not competitor_prices:
#                 logger.debug(f"  → Нет подходящих цен конкурентов → оставляем нашу: {our_price:.2f}")
#                 corrected_prices.append(our_price)
#             else:
#                 min_comp_price = min(competitor_prices)
#                 logger.debug(f"  → Минимальная цена конкурента: {min_comp_price:.2f}")

#                 if our_price > min_comp_price:
#                     new_price = max(round(min_comp_price - 2, 2), 0.0)
#                     logger.debug(f"  ✅ Наша цена выше → снижаем до: {new_price:.2f}")
#                     corrected_prices.append(new_price)
#                 else:
#                     logger.debug(f"  → Наша цена ниже или равна → оставляем: {our_price:.2f}")
#                     corrected_prices.append(our_price)

#         except Exception as e:
#             logger.error(f"❌ Ошибка при обработке строки {idx}: {e}")
#             corrected_prices.append(None)

#     # === Итоговая статистика ===
#     logger.info(f"🔧 Корректировка завершена")
#     logger.info(f"   Обработано строк: {len(corrected_prices)}")
#     logger.info(f"   Заполнено цен: {len([p for p in corrected_prices if p is not None])}")
#     logger.info(f"   Первые 10 скорректированных цен: {corrected_prices[:10]}")

#     if any_competitor_found:
#         logger.info("✅ Хотя бы один конкурент с доставкой 1–4 дня найден")
#     else:
#         logger.warning("⚠️ НИ ОДИН конкурент не попал в диапазон 1–4 дня — все цены оставлены как есть")

#     # === Добавляем колонку в DataFrame ===
#     df[corrected_price] = corrected_prices

#     # === Сохранение с форматированием ===
#     try:
#         # Сохраняем в Excel
#         df.to_excel(output_file, index=False, engine='openpyxl')
#         logger.info(f"💾 Excel сохранён: {output_file}")

#         # Загружаем workbook для форматирования
#         wb = load_workbook(output_file)
#         ws = wb.active

#         # Находим индексы колонок
#         corr_col_idx = None
#         orig_col_idx = None

#         for col in range(1, ws.max_column + 1):
#             header = ws.cell(row=1, column=col).value
#             if header == corrected_price:
#                 corr_col_idx = col
#             if header == INPUT_COL_PRICE:
#                 orig_col_idx = col

#         if not corr_col_idx:
#             logger.warning(f"❌ Колонка '{corrected_price}' не найдена в Excel — пропускаю форматирование")
#             wb.save(output_file)
#             return df

#         if not orig_col_idx:
#             logger.warning(f"❌ Колонка '{INPUT_COL_PRICE}' не найдена — пропускаю цвета")
#             wb.save(output_file)
#             return df

#         # Цвета
#         red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')  # снижена
#         green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')  # без изменений

#         # Применяем цвета
#         for row_idx in range(2, ws.max_row + 1):
#             try:
#                 orig_val = ws.cell(row=row_idx, column=orig_col_idx).value
#                 corr_val = ws.cell(row=row_idx, column=corr_col_idx).value

#                 if pd.isna(orig_val) or pd.isna(corr_val):
#                     continue

#                 try:
#                     orig_val = float(orig_val)
#                     corr_val = float(corr_val)
#                 except (ValueError, TypeError):
#                     continue

#                 cell = ws.cell(row=row_idx, column=corr_col_idx)
#                 if corr_val < orig_val:
#                     cell.fill = red_fill
#                     logger.debug(f"  → Строка {row_idx}: цена снижена → КРАСНЫЙ")
#                 else:
#                     cell.fill = green_fill
#                     logger.debug(f"  → Строка {row_idx}: цена без изменений → ЗЕЛЁНЫЙ")

#             except Exception as e:
#                 logger.debug(f"  ❌ Ошибка форматирования строки {row_idx}: {e}")
#                 continue

#         wb.save(output_file)
#         logger.info(f"✅ Файл сохранён с корректировкой цен и форматированием: {output_file}")

#     except Exception as e:
#         logger.error(f"❌ Ошибка при сохранении Excel: {e}")

#     return df