package internal

import (
	"encoding/csv"
	"fmt"
	"os"
)

// SANITIZA TEXTO PARA PREVENIR CSV INJECTION
func SanitizeCSV(val string) string {
	if val == "" {
		return val
	}
	firstChar := val[0]
	if firstChar == '=' || firstChar == '+' || firstChar == '-' || firstChar == '@' || firstChar == '\t' || firstChar == '\r' {
		return "'" + val
	}
	return val
}

// ESCRIBE LAS RECOMENDACIONES EN UN ARCHIVO CSV.
func EscribirResultados(ruta string, resultados []ResultadoEstudiante) error {
	file, err := os.Create(ruta)
	if err != nil {
		return fmt.Errorf("no se pudo crear %s: %w", ruta, err)
	}
	defer file.Close()

	// BOM PARA COMPATIBILIDAD CON EXCEL
	file.WriteString("\xEF\xBB\xBF")

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"ID_Postulante", "Rank", "Beca_Nombre", "Score",
		"Nivel", "Institucion", "Sede",
	})

	for _, res := range resultados {
		for _, rec := range res.Recomendaciones {
			writer.Write([]string{
				rec.IDPostulante,
				fmt.Sprintf("%d", rec.Rank),
				SanitizeCSV(rec.BecaNombre),
				fmt.Sprintf("%d", rec.Score),
				SanitizeCSV(rec.Nivel),
				SanitizeCSV(rec.Institucion),
				SanitizeCSV(rec.Sede),
			})
		}
	}

	return nil
}

// ESCRIBE RECOMENDACIONES AL CSV DESDE UN CANAL (FAN-IN)
func EscribirResultadosStream(ruta string, resultados <-chan ResultadoEstudiante) (int, error) {
	file, err := os.Create(ruta)
	if err != nil {
		return 0, fmt.Errorf("no se pudo crear %s: %w", ruta, err)
	}
	defer file.Close()

	file.WriteString("\xEF\xBB\xBF")
	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"ID_Postulante", "Rank", "Beca_Nombre", "Score",
		"Nivel", "Institucion", "Sede",
	})

	total := 0
	for res := range resultados {
		for _, rec := range res.Recomendaciones {
			writer.Write([]string{
				rec.IDPostulante,
				fmt.Sprintf("%d", rec.Rank),
				SanitizeCSV(rec.BecaNombre),
				fmt.Sprintf("%d", rec.Score),
				SanitizeCSV(rec.Nivel),
				SanitizeCSV(rec.Institucion),
				SanitizeCSV(rec.Sede),
			})
		}
		total++
	}

	return total, nil
}
