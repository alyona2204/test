from sqlalchemy import inspect
import sqlalchemy
import psycopg2

engine = sqlalchemy.create_engine('postgresql://postgres:*****@localhost:5432/admin')
inspector = inspect(engine)
connection = engine.connect()

# Задание 1
date_reg = connection.execute("""SELECT v_ext_ident, dt_reg FROM contracts
WHERE dt_reg >= (NOW() - INTERVAL '5 DAY');
""").fetchall()
print(date_reg)

# Задание 2
status_report = connection.execute("""SELECT v_status, count(v_status) FROM contracts
GROUP BY v_status
ORDER BY COUNT(v_status) DESC
;
""").fetchall()
print(status_report)

# Задание 3
empty_department = connection.execute("""SELECT v_name FROM department d
WHERE NOT EXISTS (SELECT 1 FROM contracts c
WHERE d.id_department = c.id_department AND v_status = 'A')
ORDER BY v_name;
""").fetchall()
print(empty_department)

# Задание 4
bills = connection.execute("""
SELECT c.v_ext_ident, b.f_sum, b.dt_event FROM contracts c, bills b
WHERE b.id_contract_inst = c.id_contract_inst and dt_event::date between '2021-09-20' AND NOW()
;
""").fetchall()
print(bills)

# Задание 8
popular_services = connection.execute("""
SELECT t.v_name, ss.v_name, COUNT(s.id_service) FROM tariff_plan t, service ss, services s
WHERE t.id_tariff_plan = s.id_tariff_plan AND s.id_service = ss.id_service
GROUP BY s.id_service, t.v_name, ss.v_name
ORDER BY COUNT(s.id_service) DESC
limit 5
;
""").fetchall()
print(popular_services)

# Задание 7
unique_services = connection.execute("""
SELECT s.v_name FROM department d, contracts c, service s, services ss
WHERE d.id_department = c.id_department AND c.id_contract_inst = ss.id_contract_inst AND ss.id_service = s.id_service
GROUP BY s.v_name
HAVING COUNT(DISTINCT d.id_department) = 1
;
""").fetchall()
print(unique_services)


# С курсорами работать не доводилось в Postgres, честно пыталась, суть поняла, а синтаксис нет
# В итоге сделала по изученным материалам и примерам, на ощупь, так сказать

# Задание 6
cur = connection.execute("""
DECLARE @curs CURSOR 
SET @curs = CURSOR SCROLL FOR 
    SELECT dt_stop FROM services
    WHERE id_service !=1234 AND id_tariff_plan = 7;
OPEN @curs
UPDATE services SET dt_stop = NOW() WHERE CURRENT OF curs;
CLOSE curs;
""").fetchall()
print(cur)

# Задание 5
cur_service = connection.execute("""
CREATE OR REPLACE PROCEDURE cursor(p_id in service.id_service%TYPE, dwr OUT CURSOR) IS
v_str VARCHAR2(1000 CHAR);
BEGIN
IF p_id IS not null then
v_str := 'SELECT s.id_service, s.v_name, count(ss.id_services)
FROM service s, services ss
WHERE s.id_service = ss.id_services AND s.id_service = ' || p_id || ' 
ORDER BY s.v_name ASC';
END IF;
OPEN dwr FOR v_str;
END cursor;
""").fetchall()
print(cur_service)





