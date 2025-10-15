SELECT
    cod_scada, celda, fecha_hora, frecuencia, pot_aparente, 
    tens_prom_s, tens_fase_neutro, tens_fase_prom, tens_ref, 
    tens_fase_r_neutro, tens_fase_sn, tens_fase_tn, tens_fase_tr
FROM 
    scada_lectura
WHERE 
    (celda LIKE 'TRA%' OR celda LIKE 'TRF%') 
    AND cod_scada LIKE 'BALNEARI/%/TRF1'
ORDER BY 
    fecha_hora ASC;
	