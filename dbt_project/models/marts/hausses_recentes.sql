-- Hausses de prix supérieures au seuil (5 % par défaut)
select
    station_id,
    ville,
    adresse,
    departement,
    carburant,
    ancien_prix,
    prix,
    round(prix - ancien_prix, 3)                          as hausse,
    round(100 * (prix - ancien_prix) / ancien_prix, 1)    as hausse_pct,
    ts
from {{ ref('stg_prix') }}
where ancien_prix is not null
  and prix > ancien_prix * (1 + {{ var('seuil_hausse') }})
