-- Données géographiques

create table Region(
    id_region int primary key,
    nom_region varchar(50)
);

create table Departement(
    id_departement int primary key,
    nom_departement varchar(50),
    id_region int,
    foreign key (id_region) references Region(id_region)
);

create table Commune(
    id_commune int primary key,
    nom_commune varchar(50),
    id_departement int,
    foreign key (id_departement) references Departement(id_departement)
);

create table Chef_lieu(
    id_commune int primary key,
    id_departement int,
    id_region int,
    foreign key (id_commune) references Commune(id_commune),
    foreign key (id_departement) references Departement(id_departement),
    foreign key (id_region) references Region(id_region)
);


-- Données statistiques population

create table Libelle_stat_pop(
    id_libelle int primary key,
    code_stat varchar(50) unique,
    libelle_stat varchar(100)
);

create table Stat_pop(
    id_stat_pop int primary key,
    code_stat varchar(50),
    annee int,
    annee_fin int default null,
    foreign key (code_stat) references Libelle_stat_pop(code_stat)
);

create table Donnee_pop(
    id_commune int ,
    id_stat_pop int,
    PRIMARY KEY (id_commune, id_stat_pop),
    valeur FLOAT,
    foreign key (id_commune) references Commune(id_commune),
    foreign key (id_stat_pop) references Stat_pop(id_stat_pop)
);


-- Données statistiques mariages
create table Libelle_stat_mariage(
    id_stat_mariage int primary key,
    code_stat varchar(50) unique,
    libelle_stat varchar(100)
);
create table Echantillon_mariage(
    id_echantillon int primary key,
    code_echantillon varchar(50),
    libelle_echantillon varchar(100),
    id_stat_mariage int,
    foreign key (id_stat_mariage) references Libelle_stat_mariage(id_stat_mariage)
);

create table Donnee_mariage(
    id_departement int ,
    id_echantillon int,
    PRIMARY KEY (id_departement, id_echantillon, type_couple),
    valeur int,
    type_couple varchar(5),
    annee int,
    foreign key (id_departement) references Departement(id_departement),
    foreign key (id_echantillon) references Echantillon_mariage(id_echantillon),
    check (type_couple in ('HF', 'HF-H', 'HF-F', 'HH', 'FF', 'HH-FF'))
);



