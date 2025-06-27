import os
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from openai import OpenAI

# --- CONFIGURACIÓN ---

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
POWER_AUTOMATE_URL = os.getenv("POWER_AUTOMATE_URL")

client = OpenAI(api_key=OPENAI_API_KEY)

estructura_modelo = """
Tablas y columnas disponibles en el modelo semántico:

Actualizacion: UltimaActualizacion
Calendario: Año, DateKey, TrimestreNumero, TrimNom, NumeroMes, NombreMes, NombreMesCorto, NumeroSemanaAño, DiaNumeroMes, NombreDiaSemana, NombreDiaSemanaCorto, AñoMes, YM, MesCalendario, DiaSemNum, AñoActual, MesActual, feriado, DiaHabil
Data Ventas: Año, Folio, NombreMedico, NombreCliente, FechaCreacion, detalle, Cantidad, frascos, DetalleProducto, DosisProducto, UnidadMedida, ValorPreparado, PrecioCosto, año_fecha, mes_fecha, idMedico, idCliente, idProducto, idFormaFarma, DIA_SEMANA, Dosis gramos, Consumo g, PrecioCostoUnitario, ValorEtiqueta, ValorEnvase, ValorMaterialEmpaque, ValorFrasco, ValorCapsulas, ValorCostoTotal, ValorLactosa, ValorTalco
Maestra_Cliente: NombreCliente, RUT, DIGITO, NOMBRE, PATERNO, MATERNO, DIRECCION, TELEFONO, CELULAR, FAX, CIUDAD, COMUNA, REGION, REGION_CORTA, fechanacimiento, fechaModificacion, RutCliente, REGION2
Maestra_Medicos: idMedico, RUT, DIGITO, NOMBRE, PATERNO, MATERNO, DIRECCION, TELEFONO, CELULAR, FAX, CIUDAD, COMUNA, REGION, REGION_CORTA, fechaModificacion, observaciones, valorAdicional, mail, idRepresentante, idUsuarioCreacion, idUsuarioModificacion, Nombre Medico, Rut Medico, Especialidad, idEspecialidad
Maestra_Productos: idProducto, FechaCreacion, detalle, UnidadMedida, fechaModificacion, codigo_ISP, precio_venta, precio_costo, controlado, dosismaxima, codigobase, sinonimo, idUsuarioCreado, precioUnico, equivalencia, unidadMedidaEquivalencia, stock, stockCritico, maximoCapsula, tipoMateriaPrima, Venta_Publico, Precio Venta mg, Precio Costo mg, Activo, diasVencimiento
Maestra_Producto_Lote: idProducto, idProductoLote, lote, FechaVencimientoLote, Activo
Maestra_Forma_Farmaceutica: detalle, fechaModificacion, idFormaFarma, precioBase, tipoCalculo, Activo, unidad, diasVencimiento, horasEntrega
Maestra_Proveedor: RUT, DIGITO, NOMBRE, DIRECCION, TELEFONO, CELULAR, FAX, CIUDAD, razon_social, idCiudad, idComuna, idProveedor
Agrupacion Cantidades por Medico: idMedico, Folio, Cantidad, frascos, FechaCreacion
Feriados: Fecha Feriado
"""

# --- FUNCIONES ---

def pregunta_a_dax(pregunta):
    prompt = f"""
        Eres un asistente experto en análisis de datos y lenguaje DAX, que ayuda a generar consultas precisas y ejecutables para Power BI.

        Dispones de un modelo de datos con las siguientes tablas y columnas:

        {estructura_modelo}

        Tu tarea es interpretar cualquier pregunta hecha en lenguaje natural por un usuario y generar una consulta DAX correcta, optimizada y ejecutable. Siempre usa los nombres exactos de tablas y columnas proporcionados.

        📌 Consideraciones clave para generar la consulta DAX:
        - Usa funciones como `SUM`, `MAX`, `COUNTROWS`, `CALCULATE`, `FILTER`, `SUMMARIZECOLUMNS`, etc., según la intención de la pregunta.
        - Si necesitas filtrar por fechas relativas (como "mes pasado", "últimos 7 días"), usa funciones como `EDATE`, `TODAY()`, `DATEADD`, y asegúrate de aplicar el filtro correctamente dentro de `CALCULATE`, sin usar `VAR` si no estás generando una medida.
        - Si el usuario se refiere a:
        - "médicos" → usa la tabla `'Maestra_Medicos'`
        - "nombre del médico" → usa la columna `'Nombre Medico'`
        - "ventas", "frascos", "precio", etc. → revisa la tabla `'Data Ventas'`
        - "productos" → usa `'Maestra_Productos'`
        - "detalle del producto" → usa `'Maestra_Productos'[detalle]`
        - "precio costo" → usa `'Data Ventas'[PrecioCosto]`
        - "fecha de creación" → usa la columna `'fechaCreacion'` de la tabla correspondiente

        ✅ Cuando uses `SELECTCOLUMNS(...)` seguido de `FILTER(...)`, recuerda que:
        - Solo puedes referenciar las columnas *renombradas* directamente por su alias (por ejemplo: `[detalle]`, no `'Maestra_Productos'[detalle]`).
        - Alternativamente, usa `CALCULATETABLE(...)` para aplicar el filtro antes de seleccionar columnas.

        ✅ Cuando el usuario solicite un **único valor agregado** (por ejemplo, el total del mes anterior o la suma general):
        - Utiliza `EVALUATE ROW(...)` o `EVALUATE { ... }` para devolver solo una fila con una etiqueta descriptiva.
        - Evita `SUMMARIZECOLUMNS` en estos casos para no devolver múltiples filas.
        - Asegúrate de que la consulta sea ejecutable directamente.

        🛑 No uses `VAR` ni `RETURN`, ya que la consulta debe ser directamente ejecutable como una sentencia `EVALUATE`.

        Consulta del usuario:
        \"{pregunta}\"

        Devuelve únicamente la consulta DAX completa y válida, sin comentarios ni explicación.
        """


    response = client.chat.completions.create(
        model="gpt-4.1",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content.strip()


def enviar_a_power_automate(dax_query):
    body = {"query": dax_query}
    headers = {"Content-Type": "application/json"}

    response = requests.post(POWER_AUTOMATE_URL, headers=headers, json=body)

    if response.status_code == 200:
        try:
            json_respuesta = response.json()
            resultado = json_respuesta.get("resultado", "Sin resultado desde Power Automate")
            return resultado
        except Exception:
            return "⚠️ Error leyendo la respuesta JSON"
    else:
        return f"❌ Error en Power Automate: {response.status_code}"


def enviar_mensaje_telegram(chat_id, mensaje):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": mensaje}
    requests.post(url, json=payload)

def formatear_respuesta(resultado):
    if resultado is None:
        return "⚠️ No se encontraron resultados para tu consulta."

    # Si es lista
    if isinstance(resultado, list):
        if len(resultado) == 0:
            return "⚠️ No se encontraron resultados para tu consulta."
        else:
            mensajes = [str(item) for item in resultado]
            return "\n".join(mensajes)

    # Si es diccionario
    if isinstance(resultado, dict):
        # Si tiene solo una clave, devuelve su valor directamente
        if len(resultado) == 1:
            return list(resultado.values())[0]

        # Si tiene varias claves, arma el listado
        mensajes = [f"🔹 {clave}: {valor}" for clave, valor in resultado.items()]
        return "\n".join(mensajes)

    # Si es string
    if isinstance(resultado, str):
        if resultado.strip() == "":
            return "⚠️ No se encontraron resultados para tu consulta."

        if "error" in resultado.lower() or "exception" in resultado.lower():
            return "❌ Hubo un problema al procesar la consulta. Por favor, revisa tu pregunta."

        # Si parece un diccionario en string
        if resultado.strip().startswith("{") and resultado.strip().endswith("}"):
            try:
                data = eval(resultado)
                if isinstance(data, dict):
                    if len(data) == 1:
                        return list(data.values())[0]
                    mensajes = [f"🔹 {clave}: {valor}" for clave, valor in data.items()]
                    return "\n".join(mensajes)
            except Exception:
                pass

        return f"📈 Resultado obtenido:\n{resultado}"

    # Si no es ninguno de los anteriores, lo convierte a string
    return str(resultado)




# --- FASTAPI APP ---

app = FastAPI()

@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    message = data.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    texto = message.get("text", "")

    if not chat_id or not texto:
        return JSONResponse(content={"ok": True})

    dax = pregunta_a_dax(texto)
    resultado_bruto = enviar_a_power_automate(dax)
    resultado_formateado = formatear_respuesta(resultado_bruto)

    respuesta = f"📊 Tu consulta fue:\n{texto}\n\n{resultado_formateado}"
    enviar_mensaje_telegram(chat_id, respuesta)

    return JSONResponse(content={"ok": True})



 