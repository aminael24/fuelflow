-- Événements bruts écrits par le consumer, nettoyés et typés
select
    cast(station_id as varchar)          as station_id,
    trim(adresse)                        as adresse,
    upper(trim(ville))                   as ville,
    code_postal,
    departement,
    code_departement,
    try_cast(latitude as double)         as latitude,
    try_cast(longitude as double)        as longitude,
    carburant,
    cast(prix as double)                 as prix,
    try_cast(ancien_prix as double)      as ancien_prix,
    cast(ts as timestamptz)              as ts
from read_parquet('{{ var("raw_path") }}', union_by_name = true)
where prix is not null
  and prix between 0.5 and 4.0          -- élimine les prix aberrants
