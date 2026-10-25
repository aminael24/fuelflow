-- Prix moyen, min, max et écart par département et carburant
select
    departement,
    carburant,
    count(*)                        as nb_stations,
    round(avg(prix), 3)             as prix_moyen,
    min(prix)                       as prix_min,
    max(prix)                       as prix_max,
    round(max(prix) - min(prix), 3) as ecart
from {{ ref('prix_actuels') }}
where departement is not null
group by all
