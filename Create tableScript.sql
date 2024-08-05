CREATE TRANSIENT TABLE IF NOT EXISTS restaurent 
(
    RestaurentID STRING,
    RestaurentName STRING,
    Rating STRING,
    Rating_Count STRING,
    Address STRING,
    Loc_No STRING,
    City STRING,
    Link STRING
);

CREATE TRANSIENT TABLE IF NOT EXISTS restaurant_menu 
(
    RestaurentID STRING,
    MenuCategory STRING,
    MenuCartItem STRING,
    MenuCartItemPrice STRING,
    MenuCartItemType STRING
);

    