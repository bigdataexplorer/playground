USE playground;
SELECT a.date, a.close, b.close
FROM stock a
JOIN stock b
ON a.date = b.date 
WHERE a.symbol = 'TSLA' and b.symbol = 'AAPL'
; 