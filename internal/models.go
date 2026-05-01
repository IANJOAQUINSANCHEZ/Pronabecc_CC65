package internal

// ESTRUCTURA DE BECA PRENORMALIZADA
type Beca struct {
	Nombre          string
	Pais            string
	Nivel           string
	TipoInstitucion string
	Sede            string
	Institucion     string
	Carrera         string
	TipoGestion     string
}

// ESTRUCTURA DE ESTUDIANTE PRENORMALIZADA
type Estudiante struct {
	IDPostulante      string
	Convocatoria      string
	TipoEstudiante    string
	IngresosMensuales float64
	Genero            string
	Distrito          string
}

// ESTRUCTURA DE RECOMENDACION
type Recomendacion struct {
	IDPostulante string
	Rank         int
	BecaNombre   string
	Score        int
	Nivel        string
	Institucion  string
	Sede         string
}

// ESTRUCTURA DE RESULTADO ESTUDIANTE
type ResultadoEstudiante struct {
	IDPostulante    string
	Recomendaciones []Recomendacion
}
