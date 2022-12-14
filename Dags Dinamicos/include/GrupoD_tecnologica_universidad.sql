SELECT university, 
	   career, 
	   inscription_date AS inscription_date, 
	   nombre AS first_name, 
	   NULL AS last_name, 
	   sexo AS gender, 
	   birth_date AS age, 
	   NULL AS postal_code, 
	   location, 
	   email 
FROM jujuy_utn
WHERE (university = 'universidad tecnológica nacional') AND (inscription_date BETWEEN '2020-09-01' and '2021-02-01');