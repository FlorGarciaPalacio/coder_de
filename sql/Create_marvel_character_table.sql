CREATE TABLE f_garcia_palacio_coderhouse.marvel_characters (
id	BIGINT primary key,
character_name VARCHAR(150),	
character_description VARCHAR,
date_modified	DATE,
resourceURI	VARCHAR(1500),
urls	VARCHAR(4000),
thumbnail	VARCHAR(4000),	
comics	VARCHAR(4000),
stories	VARCHAR(4000),
events	VARCHAR(4000),
series	VARCHAR(4000),
exec_date TIMESTAMP
);

comment on column f_garcia_palacio_coderhouse.marvel_characters.id IS 'The unique ID of the character resource.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.character_name IS 'The name of the character.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.character_description IS 'A short bio or description of the character.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.date_modified IS 'The date the resource was most recently modified.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.resourceURI IS 'The canonical URL identifier for this resource.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.urls IS 'Array[Url]	A set of public web site URLs for the resource.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.thumbnail IS 'The representative image for this character.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.comics IS 'ResourceList	A resource list containing comics which feature this character.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.stories IS 'ResourceList	A resource list of stories in which this character appears.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.events IS 'ResourceList	A resource list of events in which this character appears.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.series IS 'ResourceList	A resource list of series in which this character appears.';
comment on column f_garcia_palacio_coderhouse.marvel_characters.exec_date IS 'Timestamp of the day the API script was excecuted'
