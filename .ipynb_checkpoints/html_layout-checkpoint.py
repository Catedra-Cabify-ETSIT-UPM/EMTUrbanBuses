index_string = '''
<!DOCTYPE html>
<html class = "">
    
    <head>
        {%metas%}
        <title>NYC TAXI DATA</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bulma/0.8.0/css/bulma.min.css">
        <script defer src="https://use.fontawesome.com/releases/v5.3.1/js/all.js"></script>
        <link href='https://fonts.googleapis.com/css?family=Megrim' rel='stylesheet'>
            <style>
                .logo1 {
                    color : white;
                    font-family: 'Megrim';
                    font-size: 30px;
                }
                .navlogo {
                    background: linear-gradient(to left, #8e2de2, #4a00e0);
                }
            </style>
        {%favicon%}
        {%css%}
    </head>
    
    <body class="body">
    
        <nav class="navbar navlogo" role="navigation" aria-label="main navigation">
          <div class="navbar-brand">
            <p class="navbar-item logo1">NYC TAXI DATA VISUALIZATION</p>
          </div>
        </nav>
        
        <section class="hero"
            {%app_entry%}
        </section>
        
        <footer class="footer hero has-text-centered">
                <a class='subtitle is-4 is-primary' href="https://github.com/Catedra-Cabify-ETSIT-UPM">CÁTEDRA CABIFY ETSIT UPM</a>
                {%config%}
                {%scripts%}
                {%renderer%}
        </footer>
        
    </body>
    
</html>
'''
