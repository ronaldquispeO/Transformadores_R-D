```mermaid
flowchart TD
    %% ========== FUENTES DE DATOS ==========
    A([🏁 Inicio del Proceso]) --> B{🔄 Fuentes de Datos}
    
    B --> C[/📊 Archivos Excel/]
    B --> D[/⚡ SCADA<br>Voltaje Excitación<br>Cada 15 min/]
    B --> E[/🐍 Scripts Python<br>Extracción/]
    
    %% ========== PROCESAMIENTO ETL ==========
    C --> F{Procesamiento ETL<br>Python}
    D --> F
    E --> F
    
    F --> G[[🔧 Limpieza de Datos]]
    G --> H[[🔄 Transformaciones]]
    H --> I[[✅ Validaciones]]
    
    %% ========== ALMACENAMIENTO ==========
    I --> J{Almacenamiento<br>PostgreSQL}
    
    J --> K[🏭 BD_Transformadores]
    J --> L[📅 BD_Calendario]
    J --> M[📊 BD_Medidas DAX]
    
    %% ========== ESTRUCTURA TABLAS ==========
    K --> N{Tablas por Subíndice}
    
    N --> O[🔄 Tablas Base<br>hidga, ace, etc.]
    N --> P[📈 Tablas Extendidas<br>hidga_extendido, etc.]
    
    O --> Q[📅 Visualización<br>Pruebas Reales]
    P --> R[📊 Gráficos<br>+ Calendario]
    
    %% ========== POWER BI ==========
    Q --> S{Modelado<br>Power BI}
    R --> S
    
    S --> T[[🤝 Relaciones de Tablas]]
    T --> U[[📈 Medidas Calculadas]]
    U --> V[[🔗 Configuración<br>Drill-through]]
    
    %% ========== DASHBOARDS ==========
    V --> W{Dashboards<br>Interactivos}
    
    W --> X[📋 Tabla General<br>Vista Inicial]
    W --> Y[🔍 Páginas Detalladas<br>Drill-through]
    
    %% ========== SUBÍNDICES PRINCIPALES ==========
    Y --> Z{7 Subíndices<br>Principales}
    
    Z --> AA[🎯 DGA]
    Z --> BB[🛢️ ACE]
    Z --> CC[⚡ AIS]
    Z --> DD[🔧 ARR]
    Z --> EE[🧲 NUC]
    Z --> FF[🔌 OLTC]
    Z --> GG[🏗️ BUS]
    
    %% ========== SUB-SUBÍNDICES ==========
    CC --> AIS1[VUT]
    CC --> AIS2[ECC]
    CC --> AIS3[FUR]
    CC --> AIS4[FP]
    CC --> AIS5[CD]
    
    DD --> ARR1[ROHM]
    DD --> ARR2[RTRA]
    DD --> ARR3[RDIS]
    
    EE --> NUC1[VEX]
    EE --> NUC2[IEX]
    EE --> NUC3[RNUC]
    
    %% ========== FUTURO ==========
    GG --> HH([🚀 Machine Learning<br>Próxima Fase])
    
    HH --> II{🔮 Predicciones}
    II --> JJ[📈 Predicción de Fallas DGA]
    II --> KK[⚡ Análisis de Tendencias]
    II --> LL[🔧 Mantenimiento Predictivo]
    II --> MM[🚨 Alertas Tempranas]
    
    JJ --> NN([🏁 Fin del Proceso])

    %% ========== ESTILOS ==========
    style A fill:#4CAF50,color:white
    style NN fill:#F44336,color:white
    style B fill:#2196F3,color:white
    style F fill:#9C27B0,color:white
    style J fill:#FF9800,color:white
    style S fill:#607D8B,color:white
    style W fill:#795548,color:white
    style Z fill:#009688,color:white
    style HH fill:#E91E63,color:white
    