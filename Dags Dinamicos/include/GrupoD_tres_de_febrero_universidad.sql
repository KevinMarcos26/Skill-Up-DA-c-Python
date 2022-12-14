SELECT
	universidad AS university, 
		careers AS career, 
		fecha_de_inscripcion AS inscription_date, 
		names AS first_name, 
		NULL AS last_name, 
		sexo AS gender, 
		birth_dates AS age, 
		codigo_postal AS postal_code, 
		NULL AS LOCATION, 
		correos_electronicos AS email
FROM palermo_tres_de_febrero
WHERE (universidad = 'universidad_nacional_de_tres_de_febrero') AND (to_date(fecha_de_inscripcion, 'DD/MOM/YY') between  '2020-09-01' AND '2021-02-01');