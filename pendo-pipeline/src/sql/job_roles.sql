SELECT 
    createdByJobRole, 
    COUNT(*) 
FROM guides_raw
GROUP BY 1 ORDER BY 2 DESC