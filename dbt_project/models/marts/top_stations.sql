-- 10 stations les moins chères par département et carburant
select
    departement,
    carburant,
    rank() over (partition by departement, carburant order by prix) as rang,
    station_id,
    ville,
    adresse,
    prix,
    latitude,
    longitude,
    ts as maj
from {{ ref('prix_actuels') }}
where departement is not null
qualify rang <= 10
