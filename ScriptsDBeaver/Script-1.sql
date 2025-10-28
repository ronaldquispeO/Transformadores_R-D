SELECT
    cod_scada, celda, fecha_hora, frecuencia, pot_aparente, 
    tens_prom_s, tens_fase_neutro, tens_fase_prom, tens_ref, 
    tens_fase_r_neutro, tens_fase_sn, tens_fase_tn, tens_fase_tr
FROM 
    scada_lectura
WHERE 
    celda IN ('TRA1','TRA2','TRA3','TRA4','TRA5','TRF1','TRF2','TRF3','TRF4','TRF5')
    AND (
        cod_scada LIKE 'ALTOPRAD/60/TRF1' OR
        cod_scada LIKE 'ASIA/220/TRA1' OR
        cod_scada LIKE 'BALNEARI/220/TRA2' OR
        cod_scada LIKE 'BALNEARI/220/TRA3' OR
        cod_scada LIKE 'BALNEARI/220/TRA4' OR
        cod_scada LIKE 'CANTERA/220/TRA1' OR
        cod_scada LIKE 'MANCHAY/220/TRA1' OR
        cod_scada LIKE 'PACHACUT/220/TRA2' OR
        cod_scada LIKE 'PROGRESO/220/TRA1' OR
        cod_scada LIKE 'SBARTOLO/60/TRF2' OR
        cod_scada LIKE 'SANJUAN/220/TRA1' OR
        cod_scada LIKE 'SANJUAN/220/TRA2' OR
        cod_scada LIKE 'SROSAN/220/TRA4%' OR
        cod_scada LIKE 'SROSAN/220/TRA3%'OR
        cod_scada LIKE 'SANLUIS/220/TRA2'
    )
ORDER BY
    fecha_hora DESC;
