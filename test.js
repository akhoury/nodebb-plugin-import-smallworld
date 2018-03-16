var fs = require('fs-extra');

require('./index').testrun({
    dbhost: '127.0.0.1',
    dbport: 3306,
    dbname: 'smallworld',
    dbuser: 'root',
    dbpass: 'password',
    tablePrefix: ''
}, function(err, results) {
    fs.writeFileSync('./tmp.json', JSON.stringify(results, undefined, 2));
    process.exit(err ? 1 : 0);
});