SELECT 
    universidades as university,
    carreras as career, 
    fechas_de_inscripcion as inscription_date, 
    nombres as first_name, 
    NULL as last_name,
    sexo as gender,
    fechas_nacimiento as birth_date,
    NULL as age,
    codigos_postales as postal_code, 
    NULL as location, 
    emails as email
FROM uba_kenedy uk
Where universidades = 'universidad-de-buenos-aires' 
and to_date(fechas_de_inscripcion, 'YY-Mon-DD') between to_date('01-09-2020','DD-MM-YYY') and to_date('01-02-2021','DD-MM-YYY');
