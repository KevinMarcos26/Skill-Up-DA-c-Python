select  univiersities as university,
        carrera as career,
        inscription_dates as inscription_date,
        names as first_name,
        null as last_name,
        sexo as gender,
        fechas_nacimiento as birth_date,
        null as age,
        null as postal_code,
        localidad as location,
        email as email 
from    rio_cuarto_interamericana 
where   univiersities = 'Universidad-nacional-de-río-cuarto'
        and to_date(inscription_dates, 'yy/Mon/dd') between '2020-09-01' and '2021-02-01'  