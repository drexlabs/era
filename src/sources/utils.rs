use std::convert::TryInto;

use pallas::{
    codec::Fragment,
    network::miniprotocols::{chainsync, Point},
};

use crate::{crosscut, storage};

#[inline]
pub async fn define_chainsync_start<C: Fragment>(
    intersect: &crosscut::IntersectConfig,
    cursor: &mut storage::Cursor,
    client: &mut chainsync::Client<C>,
) -> Result<Option<Point>, crate::Error> {
    match cursor.last_point()? {
        Some(x) => {
            log::info!("found existing cursor in storage plugin: {:?}", x);
            let point = x.try_into()?;
            let (point, _) = client
                .find_intersect(vec![point])
                .await
                .map_err(crate::Error::ouroboros)?;
            return Ok(point);
        }
        None => log::info!("no cursor found in storage plugin"),
    };

    match &intersect {
        crosscut::IntersectConfig::Origin => {
            let point = client
                .intersect_origin()
                .await
                .map_err(crate::Error::ouroboros)?;
            Ok(Some(point))
        }
        crosscut::IntersectConfig::Tip => {
            let point = client
                .intersect_tip()
                .await
                .map_err(crate::Error::ouroboros)?;
            Ok(Some(point))
        }
        crosscut::IntersectConfig::Point(_, _) => {
            let point = intersect.get_point().expect("point value");
            let (point, _) = client
                .find_intersect(vec![point])
                .await
                .map_err(crate::Error::ouroboros)?;
            Ok(point)
        }
        crosscut::IntersectConfig::Fallbacks(_) => {
            let points = intersect.get_fallbacks().expect("fallback values");
            let (point, _) = client
                .find_intersect(points)
                .await
                .map_err(crate::Error::ouroboros)?;
            Ok(point)
        }
    }
}
