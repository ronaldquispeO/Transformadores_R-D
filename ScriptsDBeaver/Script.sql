SELECT 
    cod_scada,
    DATE(fecha_hora) AS fecha_horita, 
    SUM(comparacion)/96 AS comparacion_sum,
    SUM(SUM(comparacion)/96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) AS comparacion_acumulada,
    SUM(SUM(comparacion)/96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 AS vex_acumulado,
    CASE 
        WHEN SUM(SUM(comparacion)/96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 > 100 THEN 5
        WHEN SUM(SUM(comparacion)/96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 > 75 THEN 4
        WHEN SUM(SUM(comparacion)/96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 > 50 THEN 3
        WHEN SUM(SUM(comparacion)/96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 > 25 THEN 2
        ELSE 1
    END AS VEX
FROM (
    SELECT 
        cod_scada, 
        celda, 
        fecha_hora, 
        frecuencia, 
        pot_aparente, 
        tens_prom_s,
        (tens_prom_s / 210) * (60 / frecuencia) * 100 AS lado_izquierdo,
        110 - 5 * pot_aparente / 60 AS lado_derecho,
        CASE 
            WHEN (tens_prom_s / 210) * (60 / frecuencia) * 100 > (110 - 5 * pot_aparente / 60) THEN 1
            ELSE 0
        END AS comparacion
    FROM scada_lectura
    WHERE celda IN ('TRA1', 'TRA2', 'TRA3')
      AND cod_scada LIKE '%/220/%'
      AND frecuencia IS NOT NULL AND frecuencia != 0
      AND pot_aparente IS NOT NULL AND pot_aparente != 0
      AND tens_prom_s IS NOT NULL AND tens_prom_s != 0
)
GROUP BY cod_scada, fecha_horita
ORDER BY fecha_horita DESC, cod_scada ASC

