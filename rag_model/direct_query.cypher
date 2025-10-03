//separate namespace Experiment1
CREATE (:Person:Experiment1 {name: "Alice"});
CREATE (:City:Experiment1 {name: "Paris"});

MATCH (n:Law)
DETACH DELETE n

CREATE (:Point:Law1 {amended: 'Yes', old: "5. Cổ tức là khoản lợi nhuận ròng được trả cho mỗi cổ phần bằng tiền mặt hoặc bằng tài sản khác.", 
new: "5. Cổ tức là khoản lợi nhuận sau thuế được trả cho mỗi cổ phần bằng tiền hoặc bằng tài sản khác.", last_updated: "17/06/2025"});

MATCH (n: Point: Law1)
DETACH DELETE n