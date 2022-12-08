SELECT
	universidad AS university, 
		careers AS career, 
		to_date(fecha_de_inscripcion, 'DD/MOM/YY') AS inscription_date, 
		names AS first_name, 
		NULL AS last_name, 
		sexo AS gender, 
		birth_dates AS birth_date,
		NULL AS age, 
		codigo_postal AS postal_code, 
		NULL AS LOCATION, 
		correos_electronicos AS email
FROM palermo_tres_de_febrero
WHERE (universidad = '_universidad_de_palermo') AND (to_date(fecha_de_inscripcion, 'DD/MOM/YY') between  '2020-09-01' AND '2021-02-01');