SELECT 
*,
 CEILING(quartos_totais / familias) AS quartos_por_familia,
 CEILING(populacao / familias) AS populacao_por_familia,
 CASE
        WHEN proximidade_oceano = 'NEAR BAY' THEN 1
        ELSE 0
    END AS proximo_baia,
    CASE
        WHEN proximidade_oceano = 'NEAR OCEAN' THEN 1
        ELSE 0
    END AS proximo_oceano,
    CASE
        WHEN proximidade_oceano = 'ISLAND' THEN 1
        ELSE 0
    END AS ilha,
    CASE
        WHEN proximidade_oceano = 'INLAND' THEN 1
        ELSE 0
    END AS interior,
    CASE
        WHEN proximidade_oceano = '<1H OCEAN' THEN 1
        ELSE 0
    END AS distante_oceano
FROM hive_metastore.silver.informacoes_casas