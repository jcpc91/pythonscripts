import pandas as pd
from datetime import datetime

# ==== CONFIGURACIÓN ====
csv_file = "portafolio.csv"  # Ruta al CSV

# ==== LECTURA DEL CSV ====
df = pd.read_csv(csv_file)


# Validar columnas
required_cols = [
    "Símbolo de Ticker", "Nombre de la Empresa", "Tipo de Activo",
    "Cantidad de Acciones", "Precio de Compra Promedio",
    "Precio Actual de Mercado", "Ganancia/Pérdida No Realizada",
    "Porcentaje de Cartera", "Fecha de Compra"
]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"Falta la columna requerida: {col}")

df["Porcentaje de Cartera"] = df["Porcentaje de Cartera"].astype(str).str.replace("%", "", regex=False).astype(float)
# ==== CÁLCULOS ====
total_ganancia = df["Ganancia/Pérdida No Realizada"].sum()
total_invertido = (df["Cantidad de Acciones"] * df["Precio de Compra Promedio"]).sum()
total_valor_actual = (df["Cantidad de Acciones"] * df["Precio Actual de Mercado"]).sum()
total_rendimiento_pct = ((total_valor_actual - total_invertido) / total_invertido) * 100

df["Rendimiento %"] = ((df["Precio Actual de Mercado"] - df["Precio de Compra Promedio"]) / df["Precio de Compra Promedio"]) * 100
mejor_activo = df.loc[df["Rendimiento %"].idxmax()]
peor_activo = df.loc[df["Rendimiento %"].idxmin()]

activos_concentrados = df[df["Porcentaje de Cartera"] > 20]

df["Fecha de Compra"] = pd.to_datetime(df["Fecha de Compra"], errors='coerce')
df["Días en cartera"] = (datetime.today() - df["Fecha de Compra"]).dt.days

# ==== ANÁLISIS GENERAL ====
print("📊 RESUMEN GENERAL DEL PORTAFOLIO")
print(f"Valor total invertido: ${total_invertido:,.2f}")
print(f"Valor actual de mercado: ${total_valor_actual:,.2f}")
print(f"Ganancia/Pérdida total: ${total_ganancia:,.2f} ({total_rendimiento_pct:.2f}%)\n")

print("🏆 ACTIVO CON MEJOR RENDIMIENTO:")
print(f"{mejor_activo['Símbolo de Ticker']} - {mejor_activo['Nombre de la Empresa']} | {mejor_activo['Rendimiento %']:.2f}%\n")

print("⚠️ ACTIVO CON PEOR RENDIMIENTO:")
print(f"{peor_activo['Símbolo de Ticker']} - {peor_activo['Nombre de la Empresa']} | {peor_activo['Rendimiento %']:.2f}%\n")

print("📌 ACTIVOS CON ALTA CONCENTRACIÓN (>20% de cartera):")
if not activos_concentrados.empty:
    print(activos_concentrados[["Símbolo de Ticker", "Nombre de la Empresa", "Porcentaje de Cartera"]])
else:
    print("Ninguno\n")

print("⏳ TIEMPO EN CARTERA (días por activo):")
print(df[["Símbolo de Ticker", "Nombre de la Empresa", "Días en cartera"]])

# ==== PREGUNTAS INTERACTIVAS ====
print("\n❓ Responde para ajustar recomendaciones:")

horizonte = input("Horizonte de inversión (corto, mediano, largo): ").strip().lower()
riesgo = input("Tolerancia al riesgo (baja, media, alta): ").strip().lower()
liquidez = input("Necesidad de liquidez (alta, media, baja): ").strip().lower()
objetivo = input("Objetivo principal (crecimiento, dividendos, preservación): ").strip().lower()

# ==== RECOMENDACIONES AJUSTADAS ====
print("\n💡 RECOMENDACIONES PERSONALIZADAS:")

# Venta por pérdida alta
perdidas_fuertes = df[df["Rendimiento %"] < -20]
if not perdidas_fuertes.empty and riesgo != "alta":
    for _, row in perdidas_fuertes.iterrows():
        print(f"- Considerar venta o reevaluación de {row['Símbolo de Ticker']} ({row['Rendimiento %']:.2f}%) por pérdida significativa.")

# Toma de ganancias si rendimiento alto y horizonte corto
ganancias_altas = df[df["Rendimiento %"] > 30]
if not ganancias_altas.empty and horizonte == "corto":
    for _, row in ganancias_altas.iterrows():
        print(f"- {row['Símbolo de Ticker']} tiene +{row['Rendimiento %']:.2f}%. Podría tomarse ganancia dado tu horizonte corto.")

# Diversificación si concentración alta
if not activos_concentrados.empty:
    print("- La cartera está concentrada en ciertos activos. Diversificar podría reducir riesgos.")

# Enfoque en dividendos
if objetivo == "dividendos":
    dividend_stocks = df[df["Tipo de Activo"].str.contains("ETF|Acción", case=False, na=False)]
    if not dividend_stocks.empty:
        print("- Considerar reforzar posiciones en activos con historial de dividendos estables.")

# Preservación de capital
if objetivo == "preservación" and riesgo == "baja":
    print("- Mantener activos defensivos y reducir exposición a acciones volátiles.")

# Crecimiento agresivo
if objetivo == "crecimiento" and riesgo == "alta" and horizonte in ["largo", "mediano"]:
    print("- Aumentar exposición a sectores con alto potencial, como tecnología o energías renovables.")

# Liquidez alta
if liquidez == "alta":
    print("- Mantener parte de la cartera en instrumentos líquidos para disponibilidad inmediata.")

print("\n✅ Análisis completado.")
