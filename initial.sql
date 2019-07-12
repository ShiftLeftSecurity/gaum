CREATE TABLE justforfun (id int, description text, not_used text, not_used_time TIMESTAMP, CONSTRAINT therecanbeonlyone UNIQUE (id));
INSERT INTO justforfun (id, description, not_used) VALUES (1, 'first', NULL);
INSERT INTO justforfun (id, description, not_used) VALUES (2, 'second', 'meh');
INSERT INTO justforfun (id, description) VALUES (3, 'third');
INSERT INTO justforfun (id, description) VALUES (4, 'fourth');
INSERT INTO justforfun (id, description, not_used) VALUES (5, 'fift', NULL);
INSERT INTO justforfun (id, description) VALUES (6, 'sixt');
INSERT INTO justforfun (id, description) VALUES (7, 'seventh');
INSERT INTO justforfun (id, description, not_used) VALUES (8, 'eight', 'meh8');
INSERT INTO justforfun (id, description) VALUES (9, 'ninth');
INSERT INTO justforfun (id, description) VALUES (10, 'tenth');
