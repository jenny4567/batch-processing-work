DROP TABLE IF EXISTS store_visits_summary CASCADE;
DROP TABLE IF EXISTS daily_visits CASCADE;

CREATE TABLE daily_visits (
    visit_id INTEGER PRIMARY KEY,
    store_id INTEGER,
    visit_date DATE,
    visitor_count INTEGER
);

CREATE TABLE store_visits_summary (
    store_id INTEGER PRIMARY KEY,
    total_visits INTEGER,
    store_visitor_count INTEGER
);

INSERT INTO daily_visits (visit_id, store_id, visit_date, visitor_count) VALUES (1, 1, '2023-05-01', 50), (2, 1, '2023-05-02', 55), (3, 2, '2023-05-01', 60), (4, 2, '2023-05-02', 45);
