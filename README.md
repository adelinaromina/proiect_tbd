<h1>Proiect TBD - netflix prize data </h1> <br>
<h2>Colectarea si pregatirea datelor:</h2> <br>
Pentru colectare am descarcat datele folosit kagglehub. Setul de date contine 4 fisiere de tip txt numite combined_data_* ce cuprind informatii despre id ul filmului, id ul customerului, ratingul oferit si data; un fisier csv cu informatii despre id ul filmului, anul aparitiei si titlul. Setul de date mai contine si doua fisiere pentru test, fara label, probe.txt si qualifying.txt. <br>
Pentru pregatirea datelor am utilizat pandas. Deoarece fisierul csv pentru coloana de titles nu avea "" a fost nevoie de o procesare pentru a asigura integrarea tuturor datelor (cu incarcare direct in pandas cu read csv unele titluri erau impartite din cauza prezentei virgulei in titlu).
Am deschis fisierul cu with open pentru citire si apoi am split uit liniile pe baza virgulei, in cazul in care lista generata avea mai mult de 3 coloane, am facut join pentru a avea titlul filmului corect. <br>
In urma procesarii am incarcat datele intr-un dataframe cu 3 coloane. Pentru coloana year of release au existat valori NULL pe care le am eliminat, iar mai apoi am convertit coloana la int.
Dupa eliminarea valorilor null am incarcat datele in parquet. <br>
Pentru fisierele combined_data am folosit un for loop pentru a le procesa impreuna. Structura fisierului este urmatoarea: o linie cu movie id urmata de caracterul ":" apoi urmatoarele linii reprezinta informatia customer id, rating si date pentru acel movie_id. 
Am parcurs fisierul si am creat df rows pentru a include informatia de movie id la nivelul fiecarui rand apoi le am adaugat intr un dataframe. <br>
Fisierele nu au missing data, ultimul pas a fost convertirea la un format numeric, respectiv de tip data pentru coloana "date" si incarcarea intr-un fisier parquet.
