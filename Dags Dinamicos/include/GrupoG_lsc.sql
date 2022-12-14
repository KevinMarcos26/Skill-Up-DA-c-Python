SELECT 
    universities as university,
    careers as career, 
    inscription_dates as inscription_date, 
    names as first_name,
    NULL as last_name, 
    sexo as gender, 
    birth_dates as birth_date,
    NULL as age,
    NULL as postal_code, 
    locations as location, 
    emails as email
FROM lat_sociales_cine lsc
Where universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'  
and to_date(inscription_dates, 'DD-MM-YYYY') between to_date('01-09-2020','DD-MM-YYY') and to_date('01-02-2021','DD-MM-YYY');
