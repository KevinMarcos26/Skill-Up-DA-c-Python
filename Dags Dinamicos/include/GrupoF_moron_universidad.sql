select  universidad as university,
        carrerra as career,
        fechaiscripccion as inscription_date,
        nombrre as first_name,
        null as last_name,
        sexo as gender,
        nacimiento as birth_date,
        null as age,
        codgoposstal as postal_code,
        null as location,
        eemail as email
from    moron_nacional_pampa 
where   universidad = 'Universidad de morón' and
        to_date(fechaiscripccion, 'dd/mm/yyyy')  between '2020-09-01' and '2021-02-01'