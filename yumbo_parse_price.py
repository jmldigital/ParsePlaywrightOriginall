import asyncio


async def yumbo_parse_price(part: str, brand: str, logger, page) -> str | None:
    """
    Финальный парсер Yumbo-JP — минимум логов
    """
    try:
        # Заполняем форму
        part_input = page.locator("#header-search-form-part-no-id")
        await part_input.fill(part)
        await part_input.press("Enter")

        await asyncio.sleep(5)

        # Проверяем таблицу (приоритет)
        table_count = await page.locator("#partsResult").count()
        if table_count > 0:
            rows = page.locator("#partsResult tbody tr")
            row_count = await rows.count()

            for i in range(row_count):
                row = rows.nth(i)
                try:
                    # Бренд (1-й столбец)
                    brand_cell = row.locator("td:nth-child(1)")
                    brand_text = (await brand_cell.inner_text()).strip().upper()

                    # Совпадение бренда
                    if brand.upper() in brand_text:
                        # Цена (4-й столбец)
                        price_cell = row.locator(
                            "td:nth-child(4) span.money b.text-nowrap"
                        )
                        if await price_cell.count() > 0:
                            price_text = await price_cell.inner_text()
                            price_clean = price_text.replace("¥", "").strip()
                            # logger.info(f"[YUMBO] ✅ {part} | {price_clean}")
                            return price_clean
                except:
                    continue

        return None

    except Exception:
        return None
