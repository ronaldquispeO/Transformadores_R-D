SELECT
    CASE
        WHEN s.cod_scada LIKE 'ASIA/%' THEN 'Asia'
        WHEN s.cod_scada LIKE 'ALTOPRAD/%' THEN 'Alto Pradera'
        WHEN s.cod_scada LIKE 'BALNEARI/%' THEN 'Balnearios'
        WHEN s.cod_scada LIKE 'SBARTOLO/%' THEN 'San Bartolo'
        WHEN s.cod_scada LIKE 'CANTERA/%' THEN 'Cantera'
        WHEN s.cod_scada LIKE 'MANCHAY/%' THEN 'Manchay'
        WHEN s.cod_scada LIKE 'PACHACUT/%' THEN 'Pachacutec'
        WHEN s.cod_scada LIKE 'PROGRESO/%' THEN 'Progreso'
        WHEN s.cod_scada LIKE 'SANJUAN/%' THEN 'San Juan'
        WHEN s.cod_scada LIKE 'SANLUIS/%' THEN 'San Luis'
        WHEN s.cod_scada LIKE 'SROSAN/%' THEN 'Santa Rosa Nueva'
        ELSE 'Desconocido'
    END AS nombre_subestacion,
    s.celda,
    s.fecha_hora,
    s.frecuencia,
    s.pot_aparente,
    s.tens_prom_s,
    t.tension_tap,
    t.pot_instalada,
    -- Columna Lado Izquierdo
    ROUND((s.tens_prom_s / t.tension_tap) * (60 / s.frecuencia) * 100, 2) AS lado_izquierdo,
    -- Columna Lado Derecho (usando potencia instalada)
    ROUND(110 - 5 * (s.pot_aparente / t.pot_instalada), 2) AS lado_derecho,
    -- Columna Comparación: 1 si lado_izquierdo > lado_derecho, 0 si no
    CASE 
        WHEN ROUND((s.tens_prom_s / t.tension_tap) * (60 / s.frecuencia) * 100, 2) > 
             ROUND(110 - 5 * (s.pot_aparente / t.pot_instalada), 2) 
        THEN 1 
        ELSE 0 
    END AS comparacion,
    s.cod_scada
FROM 
    scada_lectura s
INNER JOIN (
    -- Subconsulta con tensión tap Y potencia instalada
    SELECT 'Alto Pradera' AS subestacion, 'TRF1' AS circuito, 65.345 AS tension_tap, 50.00 AS pot_instalada
    UNION SELECT 'Asia', 'TRA1', 210.000, 255.00
    UNION SELECT 'Balnearios', 'TRA2', 210.000, 120.00
    UNION SELECT 'Balnearios', 'TRA3', 200.000, 180.00
    UNION SELECT 'Balnearios', 'TRA4', 210.000, 180.00
    UNION SELECT 'Cantera', 'TRA1', 214.000, 25.00
    UNION SELECT 'Manchay', 'TRA1', 220.000, 120.00
    UNION SELECT 'Pachacutec', 'TRA2', 240.022, 50.00
    UNION SELECT 'Progreso', 'TRA1', 240.022, 50.00
    UNION SELECT 'San Bartolo', 'TRF2', 65.345, 25.00
    UNION SELECT 'San Juan', 'TRA1', 210.000, 180.00
    UNION SELECT 'San Juan', 'TRA2', 210.000, 180.00
    UNION SELECT 'San Luis', 'TRA2', 240.022, 50.00
    UNION SELECT 'Santa Rosa Nueva', 'TRA3', 210.000, 120.00
    UNION SELECT 'Santa Rosa Nueva', 'TRA4', 210.000, 120.00
) t ON 
    CASE
        WHEN s.cod_scada LIKE 'ASIA/%' THEN 'Asia'
        WHEN s.cod_scada LIKE 'ALTOPRAD/%' THEN 'Alto Pradera'
        WHEN s.cod_scada LIKE 'BALNEARI/%' THEN 'Balnearios'
        WHEN s.cod_scada LIKE 'SBARTOLO/%' THEN 'San Bartolo'
        WHEN s.cod_scada LIKE 'CANTERA/%' THEN 'Cantera'
        WHEN s.cod_scada LIKE 'MANCHAY/%' THEN 'Manchay'
        WHEN s.cod_scada LIKE 'PACHACUT/%' THEN 'Pachacutec'
        WHEN s.cod_scada LIKE 'PROGRESO/%' THEN 'Progreso'
        WHEN s.cod_scada LIKE 'SANJUAN/%' THEN 'San Juan'
        WHEN s.cod_scada LIKE 'SANLUIS/%' THEN 'San Luis'
        WHEN s.cod_scada LIKE 'SROSAN/%' THEN 'Santa Rosa Nueva'
        ELSE 'Desconocido'
    END = t.subestacion 
    AND s.celda = t.circuito
WHERE 
    s.celda IN ('TRA1','TRA2','TRA3','TRA4','TRA5','TRF1','TRF2','TRF3','TRF4','TRF5')
    AND (
        s.cod_scada LIKE 'ALTOPRAD/60/TRF1' OR
        s.cod_scada LIKE 'ASIA/220/TRA1' OR
        s.cod_scada LIKE 'BALNEARI/220/TRA2' OR
        s.cod_scada LIKE 'BALNEARI/220/TRA3' OR
        s.cod_scada LIKE 'BALNEARI/220/TRA4' OR
        s.cod_scada LIKE 'CANTERA/220/TRA1' OR
        s.cod_scada LIKE 'MANCHAY/220/TRA1' OR
        s.cod_scada LIKE 'PACHACUT/220/TRA2' OR
        s.cod_scada LIKE 'PROGRESO/220/TRA1' OR
        s.cod_scada LIKE 'SBARTOLO/60/TRF2' OR
        s.cod_scada LIKE 'SANJUAN/220/TRA1' OR
        s.cod_scada LIKE 'SANJUAN/220/TRA2' OR
        s.cod_scada LIKE 'SROSAN/220/TRA4%' OR
        s.cod_scada LIKE 'SROSAN/220/TRA3%' OR
        s.cod_scada LIKE 'SANLUIS/220/TRA2'
    )
ORDER BY
    s.fecha_hora DESC;