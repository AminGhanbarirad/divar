import sqlite3

connection = sqlite3.connect("divar.sqlite")
cursor = connection.cursor()

title_spam_detection_column_creation = '''
    ALTER TABLE tokens_T ADD COLUMN spam_title_C INTEGER
'''
cursor.execute(title_spam_detection_column_creation)
print("title_spam_detection_column_creation : created")

connection.commit()

title_spam_detection_C_update_query = '''
    UPDATE tokens_T
    SET spam_title_C = CASE
        WHEN title_C like "%خرید%" OR title_C like "%خراب%" OR title_C like "%مسافرت%" -- Add other conditions
        THEN 1
        ELSE 0
    END
'''
cursor.execute(title_spam_detection_C_update_query)
print("title_spam_detection_C_update_query - Rows Affected:", cursor.rowcount)
connection.commit()

status_C_update_query = '''
    UPDATE tokens_T
    SET status_C = CASE status_C
        WHEN "نو" THEN 1
        WHEN "در حد نو" THEN 2
        WHEN "کارکرده" THEN 3
        WHEN "نیازمند تعمیر" THEN 4
    END
    WHERE status_C IN ("نو", "در حد نو", "کارکرده", "نیازمند تعمیر")
'''
cursor.execute(status_C_update_query)
print("status_C_update_query - Rows Affected:", cursor.rowcount)
connection.commit()

store_C_update_query_1 = '''
    UPDATE tokens_T SET store_C = 1 WHERE store_C IN ("فروشگاه ", "فروشگاه", "فوری در فروشگاه")
'''
cursor.execute(store_C_update_query_1)
print("store_C_update_query_1 - Rows Affected:", cursor.rowcount)

store_C_update_query_2 = '''
    UPDATE tokens_T SET store_C = 0 WHERE store_C != 1 ----------------- AND store_C != "فوری "
'''
##فعلا  فوری  رو 0 میکنم
cursor.execute(store_C_update_query_2)
print("store_C_update_query_2 - Rows Affected:", cursor.rowcount)
connection.commit()

price_C_cleaning_query_1 = '''
    UPDATE tokens_T
    SET price_C = CAST(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(price_C,
    ',',''),'۰','0'),'۱','1'),'۲','2'),'۳','3'),'۴','4'),'۵','5'),'۶','6'),'۷','7'),'۸','8'),'۹','9')
    , 'تومان'       , '') AS INTEGER)
    WHERE price_C != "توافقی"
'''
cursor.execute(price_C_cleaning_query_1)
print("price_C_cleaning_query - Rows Affected:", cursor.rowcount)
connection.commit()

price_C_cleaning_query_2 = '''
    UPDATE tokens_T
    SET price_C = CAST(0 AS INTEGER)
    WHERE price_C = "توافقی"
'''
cursor.execute(price_C_cleaning_query_2)
print("price_C_cleaning_query_2 - Rows Affected:", cursor.rowcount)
connection.commit()

price_C_cleaning_query_3 = '''
    UPDATE tokens_T
    SET price_C = 0
    WHERE price_C IS NULL
'''
cursor.execute(price_C_cleaning_query_3)
print("price_C_cleaning_query_3 - Rows Affected:", cursor.rowcount)
connection.commit()


migration_description_flag_C_column_creation = '''
    ALTER TABLE tokens_T ADD COLUMN migration_description_flag_C INTEGER
'''
cursor.execute(migration_description_flag_C_column_creation)
print("migration_description_flag_C_column_creation : created")
connection.commit()

migration_description_flag_C_udate_query = '''
    UPDATE tokens_T
    SET migration_description_flag_C = CASE
        WHEN description_C like "%مهاجرت%" OR description_C like "%علت سفر%" OR description_C like "%دلیل سفر%" 
        OR title_C like "%مهاجرت%" OR title_C like "%علت سفر%" OR title_C like "%دلیل سفر%"
        THEN 1
        ELSE 0
    END
'''
cursor.execute(migration_description_flag_C_udate_query)
print("migration_description_flag_C_udate_query - Rows Affected:", cursor.rowcount)
connection.commit()


fake_price_flag_C_column_creation = '''
    ALTER TABLE tokens_T ADD COLUMN fake_price_flag_C INTEGER
'''
cursor.execute(fake_price_flag_C_column_creation)
print("fake_price_flag_C_column_creation : created")
connection.commit()

patterns = [str(i) * j for i in range(1, 10) for j in range(1, 11)]

fake_price_flag_C_query = '''
    UPDATE tokens_T
    SET fake_price_flag_C = CASE
        WHEN price_C IN ({})
        OR price_C like "%1234%"   --------- IN ( '1234', '12345', '123456', '1234567', '12345678', '123456789', '1234567890')
        OR price_C = 0
        THEN 1
        ELSE 0
    END
'''.format(','.join(['?'] * len(patterns)))

cursor.execute(fake_price_flag_C_query, patterns)
print("fake_price_flag_C_update_query - Rows Affected:", cursor.rowcount)
connection.commit()

connection.close()