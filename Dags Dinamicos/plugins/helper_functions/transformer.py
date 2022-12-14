import pandas as pd

class Transformer():
    date_formats = {
        'GrupoA_flores_universidad': '%Y-%m-%d',
        'GrupoA_villa_maria_universidad': '%d-%b-%y',
        'GrupoB_comahue_universidad': '%Y-%m-%d',
        'GrupoB_salvador_universidad': '%d-%b-%y',
        'GrupoC_jujuy_universidad':'%Y/%m/%d',
        'GrupoC_palermo_universidad':'%d/%b/%y',
        'GrupoD_tecnologica_universidad':'%Y/%m/%d',
        'GrupoD_tres_de_febrero_universidad':'%d/%b/%y',
        'GrupoE_interamericana_universidad':'%y/%b/%d',
        'GrupoE_la_pampa_universidad':'%d/%m/%Y',
        'GrupoF_moron_universidad':'%d/%m/%Y',
        'GrupoF_rio_cuarto_universidad':'%y/%b/%d',
        'GrupoG_Kennedy':'%y-%b-%d',
        'GrupoG_lsc':'%d-%m-%Y',
        'GrupoH_Cine':'%d-%m-%Y',
        'GrupoH_UBA': '%y-%b-%d',
        'GrupoI_Jujuy2': '%Y-%m-%d',
        'GrupoI_Moron2': '%d/%m/%Y',
        'GrupoJ_Pampa2': '%d/%m/%Y',
        'GrupoJ_Villa_maria2': '%d-%b-%y'
    }

    def __init__(
        self,
        university,
        logger=None,
        import_path='./files/',
        export_path='./datasets/',
        date_format=None
        ):

        self.university = university
        self.df = pd.read_csv(import_path + f"{university}_select.csv", index_col=0)
        self.export_path = export_path

        if date_format:
            self.date_format = date_format
        else:
            self.date_format = self.date_formats[university]

        self.logger = logger

        if 'fecha_nacimiento' in self.df.columns:
            self.df.rename(columns = {'fecha_nacimiento':'birth_date'}, inplace = True)
        elif not self.df['age'].isnull().values.any():
            self.df.rename(columns = {'age':'birth_date'}, inplace = True)

    def column_processor(self):
        columns_to_transform = ['university', 'career', 'email']

        if self.df['postal_code'].isnull().values.any():
            columns_to_transform.append('location')

        if not self.df['first_name'].isnull().values.any():
            columns_to_transform.append('first_name')

        if not self.df['last_name'].isnull().values.any():
            columns_to_transform.append('last_name')


        for column in columns_to_transform:
            self.df[column] = (self.df[column]
                                .str.lower()
                                .str.replace('-', ' ')
                                .str.replace('_', ' ')
                                .str.strip()
                                )

    def name_parsing(self):
        def title_parser(s):
            splits = s.split('.')
            if len(splits) == 1 or splits[1] == '':
                return s
            else:
                return splits[1].strip()
        
        self.df['first_name'] = self.df['first_name'].apply(title_parser)
        self.df[['first_name', 'last_name']] = self.df['first_name'].str.split(' ', expand = True).iloc[:,0:2]

    def gender_parsing(self):
        self.df['gender'] = self.df['gender'].str.lower()
        self.df.gender.replace(['m', 'f'], ['male', 'female'], inplace=True)

    
    def date_parser(self):
        columns_to_transform = ['inscription_date', 'birth_date']

        for column in columns_to_transform:
            self.df[column] = pd.to_datetime(self.df[column], format=self.date_format)
            self.df.style.format({column: lambda t: t.strftime("%Y-%m-%d")}) 

        if '%y' in self.date_format:
            self.df['birth_date'].where(self.df['birth_date'] < self.df['inscription_date'], self.df['birth_date'] - pd.DateOffset(years=100), inplace=True)

    def calculate_age(self): 
        today = pd.Timestamp.now()      
        self.df['age'] = self.df['birth_date'].apply(
               lambda x: today.year - x.year - 
               ((today.month, today.day) < (x.month, x.day))
               )

    def parse_locations(self):
        postal_df = pd.read_csv(f"./assets/codigos_postales.csv")
        postal_df['localidad'] = postal_df['localidad'].str.lower()

        if self.df['postal_code'].isnull().values.any():
            self.df.drop(['postal_code'],axis=1,inplace=True)
            self.df = self.df.merge(postal_df, how='left', left_on='location', right_on='localidad')
            self.df.rename(columns = {'codigo_postal':'postal_code'}, inplace = True)
            self.df = self.df.drop_duplicates(['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'location', 'email']).reset_index()

        else:
            self.df.drop(['location'], axis=1, inplace=True)
            self.df = self.df.merge(postal_df, how='left', left_on='postal_code', right_on='codigo_postal')
            self.df.rename(columns = {'localidad':'location'}, inplace = True)
    
    def to_transform(self):
        if self.logger:
            self.logger.info('Inicia proceso de transformación de los datos')

        try:
            self.column_processor()
            self.name_parsing()            
            self.gender_parsing() 
            self.date_parser()
            self.calculate_age()
            self.parse_locations()

            self.df = self.df[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]

            self.df.to_csv(self.export_path + f'{self.university}_process.txt')

            if self.logger:
                self.logger.info('Se creo archivo txt con la información transformada')
            
        except Exception as e:
            if self.logger:
                self.logger.info('ERROR al transformar los datos')
                self.logger.error(e)



    