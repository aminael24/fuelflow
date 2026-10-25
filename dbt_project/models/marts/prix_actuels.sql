-- Dernier prix connu pour chaque station et carburant
select *
from {{ ref('stg_prix') }}
qualify row_number() over (
    partition by station_id, carburant
    order by ts desc
) = 1
