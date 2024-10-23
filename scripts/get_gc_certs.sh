#!/bin/bash
pwd=$(pwd)
rm -rf /tmp/gccerts
mkdir -p "/tmp/gccerts"
cd "/tmp/gccerts" || exit 1
powershell.exe -c '$certs = Get-ChildItem "cert:\LocalMachine" -recurse | Where-Object { $_.Subject -match "O=Glencore International AG" }
$seen=@()
foreach ($cert in $certs) {
    $name=$cert.Subject -replace "[\W]", "_"
    $thumbprint=$cert.Thumbprint
    if ($seen.Contains($thumbprint)) {
        Write-Output "already seen $thumbprint, skipping"
    }else {
        Write-Output "exporting" $name, $thumbprint
        Export-Certificate -Cert @($cert)[0] -FilePath "$name.cer" -Type CERT
        $seen+=,$thumbprint
    }
    Write-Output "-----------------------"
}'

for f in *.cer; do
    sudo openssl x509 -inform der -in "${f}" -out "${f%.*}.crt"
done
sudo cp ./*.crt /usr/local/share/ca-certificates/
mkdir -p "$pwd/.certs"
cp ./*.crt "$pwd/.certs"
sudo update-ca-certificates
