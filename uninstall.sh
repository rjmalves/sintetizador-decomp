#! /bin/bash

USERINSTALLDIR=/home/pem/rotinas
INSTALLDIR=${USERINSTALLDIR}/sintetizador-decomp
echo "Removendo arquivos da instalação em ${INSTALLDIR}" 
[ -d $INSTALLDIR ] && rm -r $INSTALLDIR

EXECPATH=/usr/bin/sintetizador-decomp
echo "Removendo executável em ${EXECPATH}" 
[ -f $EXECPATH ] && rm $EXECPATH
echo "Finalizando..."
