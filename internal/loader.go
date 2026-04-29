package internal

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// LEE Y CARGA LAS BECAS EN UN SLICE DE PUNTEROS, LUEGO PRENORMALIZA STRINGS A MAYUSCULAS
func CargarBecas(ruta string) ([]*Beca, error) {
	file, err := os.Open(ruta)
	if err != nil {
		return nil, fmt.Errorf("no se pudo abrir %s: %w", ruta, err)
	}
	defer file.Close()

	lector := csv.NewReader(file)
	lector.FieldsPerRecord = -1
	lector.LazyQuotes = true

	if _, err := lector.Read(); err != nil {
		return nil, fmt.Errorf("error leyendo cabecera: %w", err)
	}

	becas := make([]*Beca, 0, 1400000)
	for {
		fila, err := lector.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(fila) < 9 {
			continue
		}

		nombre := strings.TrimSpace(fila[0])
		if nombre == "" {
			continue
		}

		becas = append(becas, &Beca{
			Nombre:          strings.ToUpper(nombre),
			Pais:            strings.TrimSpace(fila[1]),
			Nivel:           strings.ToUpper(strings.TrimSpace(fila[2])),
			TipoInstitucion: strings.TrimSpace(fila[3]),
			Sede:            strings.ToUpper(strings.TrimSpace(fila[4])),
			Institucion:     strings.TrimSpace(fila[5]),
			Carrera:         strings.TrimSpace(fila[6]),
			TipoGestion:     strings.ToUpper(strings.TrimSpace(fila[8])),
		})
	}

	return becas, nil
}

// LEE Y CARGA LOS ESTUDIANTES EN UN SLICE DE PUNTEROS, LUEGO PRENORMALIZA STRINGS A MAYUSCULAS
func CargarEstudiantes(ruta string) ([]*Estudiante, error) {
	file, err := os.Open(ruta)
	if err != nil {
		return nil, fmt.Errorf("no se pudo abrir %s: %w", ruta, err)
	}
	defer file.Close()

	lector := csv.NewReader(file)
	lector.FieldsPerRecord = -1
	lector.LazyQuotes = true

	if _, err := lector.Read(); err != nil {
		return nil, fmt.Errorf("error leyendo cabecera: %w", err)
	}

	estudiantes := make([]*Estudiante, 0, 130000)
	for {
		fila, err := lector.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(fila) < 15 {
			continue
		}

		idPostulante := strings.TrimSpace(fila[2])
		if idPostulante == "" {
			continue
		}

		ingresos := 0.0
		if val := strings.TrimSpace(fila[9]); val != "" {
			ingresos, _ = strconv.ParseFloat(val, 64)
		}

		estudiantes = append(estudiantes, &Estudiante{
			IDPostulante:      idPostulante,
			Convocatoria:      strings.ToUpper(strings.TrimSpace(fila[3])),
			TipoEstudiante:    strings.ToUpper(strings.TrimSpace(fila[4])),
			IngresosMensuales: ingresos,
			Genero:            strings.TrimSpace(fila[13]),
			Distrito:          strings.ToUpper(strings.TrimSpace(fila[14])),
		})
	}

	return estudiantes, nil
}

// CLAVE COMPUESTA NIVEL PARA PODA O(1).
func IndexarBecasPorNivel(becas []*Beca) map[string][]*Beca {
	indice := make(map[string][]*Beca)
	for _, b := range becas {
		indice[b.Nivel] = append(indice[b.Nivel], b)
	}
	return indice
}

// CREA UN INDICE MULTI-CLAVE POR NIVEL+TIPOGESTIÓN, PERMITE PODA MÁS AGRESIVA
func IndexarBecasCompuesto(becas []*Beca) map[string][]*Beca {
	indice := make(map[string][]*Beca)
	for _, b := range becas {
		// CLAVE COMPUESTA
		clave := b.Nivel + "|" + b.TipoGestion
		indice[clave] = append(indice[clave], b)
		// TAMBIÉN INDEXAR SOLO POR NIVEL COMO FALLBACK
		indice[b.Nivel] = append(indice[b.Nivel], b)
	}
	return indice
}
