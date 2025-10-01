# Samba Data Engineering ERD 



```samba
erDiagram
    DIM_DATE {
        bigint date_key PK
        date date
        int day
        int month
        int year
        int quarter
        string day_name
        boolean is_weekend
    }

    DIM_PRODUCT {
        bigint product_key PK
        varchar product_id
        varchar product_name
        varchar category
    }

    DIM_CITY {
        bigint city_id PK
        varchar city_name
    }

    DIM_BRANCH {
        bigint branch_key PK
        varchar branch_id
        bigint city_id FK
    }

    DIM_STAFF {
        bigint staff_key PK
        varchar staff_id
        varchar employee_id
        varchar first_name
        varchar last_name
        varchar role
    }

    FACT_SALES {
        bigint sale_id PK
        bigint date_key FK
        bigint product_key FK
        bigint branch_key FK
        bigint staff_key FK
        timestamp sale_ts
        int volume_sold
        decimal price
        decimal cost
        decimal revenue
    }

    DIM_BRANCH ||--o{ DIM_CITY : "located_in"
    FACT_SALES }o--|| DIM_DATE : "date_key -> date"
    FACT_SALES }o--|| DIM_PRODUCT : "product_key -> product"
    FACT_SALES }o--|| DIM_BRANCH : "branch_key -> branch"
    FACT_SALES }o--|| DIM_STAFF : "staff_key -> staff"
```

